defmodule Upkeep.Ecto.QueryDeps do
  @moduledoc """
  Extracts runtime invalidation dependencies from Ecto queries.

  This intentionally starts with the Phoenix common path: schema-backed queries
  with equality filters. Unsupported shapes are treated as broad dependencies
  on the schemas Upkeep can see, preserving correctness at the cost of precision.
  """

  @actions [:inserted, :updated, :deleted]
  defstruct bindings: %{},
            schemas: MapSet.new(),
            fields: %{},
            equality_filters: %{},
            broad?: false,
            warnings: []

  def from_query(%Ecto.Query{} = query) do
    bindings = Upkeep.Ecto.QueryDeps.Bindings.from_query(query)

    %__MODULE__{bindings: bindings, schemas: bindings |> Map.values() |> MapSet.new()}
    |> collect_query_fields(query)
    |> collect_where_equalities(query.wheres)
    |> collect_preload_deps(query)
    |> collect_subquery_deps(query)
    |> mark_broad_for_unsupported(query)
  end

  def from_query(_query), do: %__MODULE__{}

  def interest_keys(query_or_deps)

  def interest_keys(%Ecto.Query{} = query), do: query |> from_query() |> interest_keys()

  def interest_keys(%__MODULE__{} = deps) do
    deps.schemas
    |> Enum.flat_map(fn schema ->
      value_sets = equality_value_sets(deps, schema)

      for action <- @actions, values <- interest_values(deps, value_sets) do
        notification = %{name: action, schema: schema}

        if values == :broad do
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
      filters = equality_filters(deps, change.schema)
      deps.broad? or filters == %{} or change_matches_filters?(change, filters)
    else
      false
    end
  end

  def matches_change?(_deps, _event), do: false

  defp collect_query_fields(deps, query) do
    query
    |> Upkeep.Ecto.QueryDeps.Expressions.all()
    |> Enum.reduce(deps, fn expr, deps -> collect_fields(deps, expr) end)
  end

  defp collect_fields(deps, expr) do
    {_expr, deps} =
      Macro.prewalk(expr, deps, fn
        node, deps ->
          case Upkeep.Ecto.QueryDeps.Expressions.field_ref(node) do
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
      |> extract_equality_filters(params)
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

  defp extract_equality_filters(expr, params) do
    {_expr, filters} =
      Macro.prewalk(expr, [], fn
        {:==, _meta, [left, right]} = node, filters ->
          filters = maybe_add_equality_filter(filters, left, right, params)
          filters = maybe_add_equality_filter(filters, right, left, params)
          {node, filters}

        {:in, _meta, [left, right]} = node, filters ->
          filters = maybe_add_membership_filter(filters, left, right, params)
          {node, filters}

        node, filters ->
          {node, filters}
      end)

    filters
  end

  defp maybe_add_equality_filter(filters, field_side, value_side, params) do
    with {binding, field} <- Upkeep.Ecto.QueryDeps.Expressions.field_ref(field_side),
         {:ok, value} <- equality_value(value_side, params) do
      [{binding, field, value} | filters]
    else
      _ -> filters
    end
  end

  defp maybe_add_membership_filter(filters, field_side, value_side, params) do
    with {binding, field} <- Upkeep.Ecto.QueryDeps.Expressions.field_ref(field_side),
         {:ok, values} <- membership_values(value_side, params) do
      [{binding, field, values} | filters]
    else
      _ -> filters
    end
  end

  defp equality_value({:^, _meta, [index]}, params), do: Map.fetch(params, index)
  defp equality_value(%Ecto.Query.Tagged{value: value}, _params), do: {:ok, value}

  defp equality_value(value, _params) when is_binary(value) or is_number(value) or is_atom(value),
    do: {:ok, value}

  defp equality_value(_value, _params), do: :error

  defp membership_values({:^, _meta, [index]}, params) do
    case Map.fetch(params, index) do
      {:ok, values} when is_list(values) -> {:ok, values}
      {:ok, value} -> {:ok, [value]}
      :error -> :error
    end
  end

  defp membership_values(values, params) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, values} ->
      case equality_value(value, params) do
        {:ok, value} -> {:cont, {:ok, [value | values]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      :error -> :error
    end
  end

  defp membership_values(_value, _params), do: :error

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
        values = List.wrap(value)

        filters =
          Map.update(deps.equality_filters, schema, %{field => values}, fn schema_filters ->
            Map.update(schema_filters, field, values, fn existing ->
              Enum.uniq(existing ++ values)
            end)
          end)

        %{deps | equality_filters: filters}

      :error ->
        deps
    end
  end

  defp mark_broad_for_unsupported(deps, query) do
    query
    |> Upkeep.Ecto.QueryDeps.Expressions.all()
    |> Enum.reduce(deps, fn expr, deps ->
      if Upkeep.Ecto.QueryDeps.Expressions.unsupported?(expr) do
        %{deps | broad?: true, warnings: ["unsupported query expression" | deps.warnings]}
      else
        deps
      end
    end)
  end

  defp collect_subquery_deps(deps, query) do
    query
    |> Upkeep.Ecto.QueryDeps.Expressions.structs()
    |> Enum.flat_map(&Map.get(&1, :subqueries, []))
    |> Enum.reduce(deps, fn %Ecto.SubQuery{query: query}, deps ->
      merge_deps(deps, from_query(query))
    end)
  end

  defp collect_preload_deps(deps, query) do
    query
    |> preload_schemas()
    |> Enum.reduce(deps, fn schema, deps ->
      %{deps | schemas: MapSet.put(deps.schemas, schema)}
    end)
  end

  defp preload_schemas(query) do
    root_schema = Map.get(Upkeep.Ecto.QueryDeps.Bindings.from_query(query), 0)

    query.preloads
    |> schemas_for_preloads(root_schema)
    |> Enum.concat(schemas_for_assocs(query.assocs, query))
    |> Enum.uniq()
  end

  defp schemas_for_preloads(_preloads, nil), do: []

  defp schemas_for_preloads(preloads, owner_schema) when is_list(preloads) do
    Enum.flat_map(preloads, fn
      assoc when is_atom(assoc) ->
        association_schemas(owner_schema, assoc)

      {assoc, nested} when is_atom(assoc) ->
        case association_schemas(owner_schema, assoc) do
          [] -> []
          [schema | _] = schemas -> schemas ++ schemas_for_preloads(List.wrap(nested), schema)
        end

      _other ->
        []
    end)
  end

  defp schemas_for_preloads(_preloads, _owner_schema), do: []

  defp schemas_for_assocs(assocs, query) when is_list(assocs) do
    bindings = Upkeep.Ecto.QueryDeps.Bindings.from_query(query)

    Enum.flat_map(assocs, fn
      {assoc, {binding, nested}} when is_atom(assoc) and is_integer(binding) ->
        case Map.fetch(bindings, binding) do
          {:ok, schema} -> [schema | schemas_for_preloads(List.wrap(nested), schema)]
          :error -> []
        end

      _other ->
        []
    end)
  end

  defp schemas_for_assocs(_assocs, _query), do: []

  defp association_schemas(owner_schema, assoc) when is_atom(owner_schema) and is_atom(assoc) do
    with true <- function_exported?(owner_schema, :__schema__, 2),
         association when not is_nil(association) <- owner_schema.__schema__(:association, assoc) do
      [Map.get(association, :related), Map.get(association, :join_through)]
      |> Enum.filter(&((is_atom(&1) and not is_nil(&1)) or is_binary(&1)))
    else
      _ -> []
    end
  rescue
    _ -> []
  end

  defp merge_deps(left, right) do
    %{
      left
      | schemas: MapSet.union(left.schemas, right.schemas),
        fields: merge_mapsets(left.fields, right.fields),
        equality_filters: merge_filters(left.equality_filters, right.equality_filters),
        broad?: left.broad? or right.broad?,
        warnings: Enum.uniq(left.warnings ++ right.warnings)
    }
  end

  defp merge_mapsets(left, right) do
    Map.merge(left, right, fn _schema, left_fields, right_fields ->
      MapSet.union(left_fields, right_fields)
    end)
  end

  defp merge_filters(left, right) do
    Map.merge(left, right, fn _schema, left_filters, right_filters ->
      Map.merge(left_filters, right_filters, fn _field, left_values, right_values ->
        Enum.uniq(left_values ++ right_values)
      end)
    end)
  end

  defp interest_values(%{broad?: true}, _value_sets), do: [:broad]
  defp interest_values(_deps, []), do: [:broad]
  defp interest_values(_deps, value_sets), do: value_sets

  defp equality_filters(deps, schema) do
    deps.equality_filters
    |> Map.get(schema, %{})
  end

  defp equality_value_sets(deps, schema) do
    case equality_filters(deps, schema) do
      filters when map_size(filters) == 0 ->
        []

      filters ->
        filters
        |> Map.to_list()
        |> Enum.sort()
        |> Enum.reduce([[]], fn
          {_field, []}, _combinations ->
            []

          {field, values}, combinations ->
            for combination <- combinations, value <- values do
              [{field, value} | combination]
            end
        end)
        |> Enum.map(&Enum.sort/1)
    end
  end

  defp change_matches_filters?(change, filters) do
    change
    |> Upkeep.Change.field_sets()
    |> Enum.any?(fn fields ->
      Enum.all?(filters, fn {field, values} -> Enum.member?(values, Map.get(fields, field)) end)
    end)
  end
end
