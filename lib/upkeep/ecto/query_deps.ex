defmodule Upkeep.Ecto.QueryDeps do
  @moduledoc """
  Extracts runtime invalidation dependencies from Ecto queries.

  This intentionally starts with the Phoenix common path: schema-backed queries
  with equality filters. Unsupported shapes are treated as broad dependencies
  on the schemas Upkeep can see, preserving correctness at the cost of precision.
  """

  @actions [:inserted, :updated, :deleted]
  @unsupported_nodes [:or, :fragment]

  defstruct bindings: %{},
            schemas: MapSet.new(),
            fields: %{},
            equality_filters: %{},
            broad?: false,
            warnings: []

  def from_query(%Ecto.Query{} = query) do
    bindings = bindings(query)

    %__MODULE__{bindings: bindings, schemas: bindings |> Map.values() |> MapSet.new()}
    |> collect_query_fields(query)
    |> collect_where_equalities(query.wheres)
    |> mark_broad_for_unsupported(query)
  end

  def from_query(_query), do: %__MODULE__{}

  def interest_keys(query_or_deps)

  def interest_keys(%Ecto.Query{} = query), do: query |> from_query() |> interest_keys()

  def interest_keys(%__MODULE__{} = deps) do
    deps.schemas
    |> Enum.flat_map(fn schema ->
      values = equality_values(deps, schema)

      for action <- @actions do
        notification = %{name: action, schema: schema}

        if deps.broad? or values == [] do
          Upkeep.Source.notification_key(notification)
        else
          Upkeep.Source.notification_key(notification, values)
        end
      end
    end)
  end

  def interest_keys(_query), do: []

  def matches_change?(query_or_deps, event)

  def matches_change?(%Ecto.Query{} = query, event),
    do: query |> from_query() |> matches_change?(event)

  def matches_change?(%__MODULE__{} = deps, %Upkeep.Change{} = change)
      when change.name in @actions do
    if MapSet.member?(deps.schemas, change.schema) do
      values = equality_values(deps, change.schema)
      deps.broad? or values == [] or change_matches_values?(change, values)
    else
      false
    end
  end

  def matches_change?(_deps, _event), do: false

  defp bindings(query) do
    from_binding =
      case source_schema(query.from && query.from.source) do
        nil -> []
        schema -> [{0, schema}]
      end

    join_bindings =
      query.joins
      |> Enum.with_index(1)
      |> Enum.flat_map(fn {join, index} ->
        case source_schema(join.source) do
          nil -> []
          schema -> [{index, schema}]
        end
      end)

    Map.new(from_binding ++ join_bindings)
  end

  defp source_schema({_source, schema}) when is_atom(schema) and not is_nil(schema), do: schema
  defp source_schema(_source), do: nil

  defp collect_query_fields(deps, query) do
    query
    |> query_exprs()
    |> Enum.reduce(deps, fn expr, deps -> collect_fields(deps, expr) end)
  end

  defp query_exprs(query) do
    [
      Enum.map(query.joins, & &1.on),
      query.wheres,
      query.order_bys,
      query.group_bys,
      query.havings,
      List.wrap(query.select)
    ]
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
    |> Enum.map(& &1.expr)
  end

  defp collect_fields(deps, expr) do
    {_expr, deps} =
      Macro.prewalk(expr, deps, fn
        node, deps ->
          case field_ref(node) do
            {binding, field} -> {node, put_field(deps, binding, field)}
            nil -> {node, deps}
          end
      end)

    deps
  end

  defp collect_where_equalities(deps, wheres) do
    Enum.reduce(wheres, deps, fn where, deps ->
      params = params_by_index(where.params)

      where.expr
      |> equality_filters(params)
      |> Enum.reduce(deps, fn {binding, field, value}, deps ->
        deps
        |> put_field(binding, field)
        |> put_equality_filter(binding, field, value)
      end)
    end)
  end

  defp params_by_index(params) do
    params
    |> Enum.with_index()
    |> Map.new(fn {{value, _type}, index} -> {index, value} end)
  end

  defp equality_filters(expr, params) do
    {_expr, filters} =
      Macro.prewalk(expr, [], fn
        {:==, _meta, [left, right]} = node, filters ->
          filters = maybe_add_equality_filter(filters, left, right, params)
          filters = maybe_add_equality_filter(filters, right, left, params)
          {node, filters}

        node, filters ->
          {node, filters}
      end)

    filters
  end

  defp maybe_add_equality_filter(filters, field_side, value_side, params) do
    with {binding, field} <- field_ref(field_side),
         {:ok, value} <- equality_value(value_side, params) do
      [{binding, field, value} | filters]
    else
      _ -> filters
    end
  end

  defp equality_value({:^, _meta, [index]}, params), do: Map.fetch(params, index)
  defp equality_value(%Ecto.Query.Tagged{value: value}, _params), do: {:ok, value}

  defp equality_value(value, _params) when is_binary(value) or is_number(value) or is_atom(value),
    do: {:ok, value}

  defp equality_value(_value, _params), do: :error

  defp field_ref({{:., _dot_meta, [{:&, _binding_meta, [binding]}, field]}, _meta, []})
       when is_integer(binding) and is_atom(field) do
    {binding, field}
  end

  defp field_ref(_node), do: nil

  defp put_field(deps, binding, field) do
    case Map.fetch(deps.bindings, binding) do
      {:ok, schema} ->
        fields = Map.update(deps.fields, schema, MapSet.new([field]), &MapSet.put(&1, field))
        %{deps | fields: fields}

      :error ->
        deps
    end
  end

  defp put_equality_filter(deps, binding, field, value) do
    case Map.fetch(deps.bindings, binding) do
      {:ok, schema} ->
        filters =
          Map.update(deps.equality_filters, schema, %{field => value}, fn schema_filters ->
            Map.put(schema_filters, field, value)
          end)

        %{deps | equality_filters: filters}

      :error ->
        deps
    end
  end

  defp mark_broad_for_unsupported(deps, query) do
    query
    |> query_exprs()
    |> Enum.reduce(deps, fn expr, deps ->
      if unsupported_expr?(expr) do
        %{deps | broad?: true, warnings: ["unsupported query expression" | deps.warnings]}
      else
        deps
      end
    end)
  end

  defp unsupported_expr?(expr) do
    {_expr, unsupported?} =
      Macro.prewalk(expr, false, fn
        {node, _meta, _args} = expr, _unsupported? when node in @unsupported_nodes ->
          {expr, true}

        expr, unsupported? ->
          {expr, unsupported?}
      end)

    unsupported?
  end

  defp equality_values(deps, schema) do
    deps.equality_filters
    |> Map.get(schema, %{})
    |> Map.to_list()
    |> Enum.sort()
  end

  defp change_matches_values?(change, values) do
    change
    |> Upkeep.Change.field_sets()
    |> Enum.any?(fn fields ->
      Enum.all?(values, fn {field, value} -> Map.get(fields, field) == value end)
    end)
  end
end
