defmodule Upkeep.Ecto.Source.QueryDeps do
  @moduledoc false

  @actions [:inserted, :updated, :deleted]
  defstruct bindings: %{},
            schemas: MapSet.new(),
            fields: %{},
            equality_filters: %{},
            broad_reasons: %{},
            broad?: false,
            warnings: []

  def from_query(%Ecto.Query{} = query) do
    bindings = Upkeep.Ecto.Source.QueryDeps.Bindings.from_query(query)

    %__MODULE__{bindings: bindings, schemas: bindings |> Map.values() |> MapSet.new()}
    |> collect_query_fields(query)
    |> collect_where_equalities(query.wheres)
    |> collect_preload_deps(query)
    |> collect_subquery_deps(query)
    |> mark_broad_for_unsupported(query)
  end

  def from_query(_query), do: %__MODULE__{}

  def coverage(%__MODULE__{} = deps) do
    {unknown, warnings} = Enum.split_with(deps.warnings, &unknown_warning?/1)

    deps.schemas
    |> Enum.reduce(
      Upkeep.Source.Coverage.new(nil, %{}, unknown: unknown, warnings: warnings),
      fn schema, coverage ->
        reasons = broad_reasons(deps, schema)
        filter_sets = equality_filter_sets(deps, schema)

        cond do
          reasons != [] ->
            Enum.reduce(reasons, coverage, fn reason, coverage ->
              append_broad(coverage, schema, reason)
            end)

          filter_sets == [] ->
            append_broad(coverage, schema, :no_precise_filters)

          true ->
            append_precise(coverage, schema, filter_set_fields(filter_sets))
        end
      end
    )
  end

  def coverage(%Ecto.Query{} = query), do: query |> from_query() |> coverage()

  def label(%__MODULE__{} = deps) do
    schemas =
      deps.schemas
      |> MapSet.to_list()
      |> Enum.map(&module_label/1)
      |> join_or_empty()

    filters =
      deps.equality_filters
      |> Enum.flat_map(fn {schema, filters} ->
        Enum.map(filters, fn {field, values} ->
          "#{module_label(schema)}.#{field}=#{inspect(values, inspect_opts())}"
        end)
      end)

    mode =
      cond do
        deps.broad? -> "broad"
        filters == [] -> "broad"
        true -> "precise"
      end

    "#{mode} query on #{schemas}#{if(filters == [], do: "", else: " filtered by #{Enum.join(filters, ", ")}")}"
  end

  def interest_keys(query_or_deps)

  def interest_keys(%Ecto.Query{} = query), do: query |> from_query() |> interest_keys()

  def interest_keys(%__MODULE__{} = deps) do
    deps.schemas
    |> Enum.flat_map(fn schema ->
      value_sets = interest_values(deps, schema)

      for action <- @actions, values <- value_sets do
        notification = %{name: action, schema: schema}

        if values == :broad do
          Upkeep.Source.Keys.notification_key(notification)
        else
          Upkeep.Source.Keys.notification_key(notification, values)
        end
      end
    end)
  end

  def interest_keys(_query), do: []

  def coarse_keys(%__MODULE__{schemas: schemas}) do
    for schema <- schemas, action <- @actions do
      {action, schema}
    end
  end

  def coarse_keys(_deps), do: []

  def matches_change?(query_or_deps, event)

  def matches_change?(%Ecto.Query{} = query, event),
    do: query |> from_query() |> matches_change?(event)

  def matches_change?(%__MODULE__{} = deps, %Upkeep.Change{} = change)
      when change.name in @actions do
    if MapSet.member?(deps.schemas, change.schema) do
      filter_sets = equality_filter_sets(deps, change.schema)

      Upkeep.Change.broad_update?(change) or broad_schema?(deps, change.schema) or
        filter_sets == [] or
        Enum.any?(filter_sets, &change_matches_filters?(change, &1))
    else
      false
    end
  end

  def matches_change?(_deps, _event), do: false

  defp collect_query_fields(deps, query) do
    query
    |> Upkeep.Ecto.Source.QueryDeps.Expressions.all()
    |> Enum.reduce(deps, fn expr, deps -> collect_fields(deps, expr) end)
  end

  defp collect_fields(deps, expr) do
    {_expr, deps} =
      Macro.prewalk(expr, deps, fn
        node, deps ->
          case Upkeep.Ecto.Source.QueryDeps.Expressions.field_ref(node) do
            {binding, field} -> {node, put_field(deps, binding, field)}
            nil -> {node, deps}
          end
      end)

    deps
  end

  defp collect_where_equalities(deps, wheres) do
    Enum.reduce(wheres, deps, fn where, deps ->
      params = params_by_index(where.params)

      case extract_equality_filter_sets(where.expr, params) do
        {:ok, filter_sets} ->
          put_equality_filter_sets(deps, filter_sets)

        :error ->
          deps
      end
    end)
  end

  defp params_by_index(params) do
    params
    |> Enum.with_index()
    |> Map.new(fn {{value, _type}, index} -> {index, value} end)
  end

  defp extract_equality_filter_sets({:and, _meta, [left, right]}, params) do
    with {:ok, left_sets} <- extract_equality_filter_sets(left, params),
         {:ok, right_sets} <- extract_equality_filter_sets(right, params) do
      {:ok, combine_filter_sets(left_sets, right_sets)}
    end
  end

  defp extract_equality_filter_sets({:or, _meta, [left, right]}, params) do
    with {:ok, [_ | _] = left_sets} <- extract_equality_filter_sets(left, params),
         {:ok, [_ | _] = right_sets} <- extract_equality_filter_sets(right, params) do
      {:ok, left_sets ++ right_sets}
    else
      _ -> :error
    end
  end

  defp extract_equality_filter_sets({:==, _meta, [left, right]}, params) do
    []
    |> maybe_add_equality_filter(left, right, params)
    |> maybe_add_equality_filter(right, left, params)
    |> filter_result()
  end

  defp extract_equality_filter_sets({:in, _meta, [left, right]}, params) do
    []
    |> maybe_add_membership_filter(left, right, params)
    |> filter_result()
  end

  defp extract_equality_filter_sets(_expr, _params), do: {:ok, []}

  defp combine_filter_sets([], right_sets), do: right_sets
  defp combine_filter_sets(left_sets, []), do: left_sets

  defp combine_filter_sets(left_sets, right_sets) do
    for left <- left_sets, right <- right_sets do
      left ++ right
    end
  end

  defp filter_result([]), do: {:ok, []}
  defp filter_result(filters), do: {:ok, [filters]}

  defp put_equality_filter_sets(deps, filter_sets) do
    Enum.reduce(filter_sets, deps, fn filter_set, deps ->
      filter_set
      |> filters_by_schema(deps)
      |> Enum.reduce(deps, fn {schema, filters}, deps ->
        filters =
          Enum.reduce(filters, %{}, fn {_binding, field, value}, filters ->
            values = List.wrap(value)

            Map.update(filters, field, values, fn existing ->
              Enum.uniq(existing ++ values)
            end)
          end)

        deps
        |> put_fields(filter_set)
        |> put_equality_filter_set(schema, filters)
      end)
    end)
  end

  defp maybe_add_equality_filter(filters, field_side, value_side, params) do
    with {binding, field} <- Upkeep.Ecto.Source.QueryDeps.Expressions.field_ref(field_side),
         {:ok, value} <- equality_value(value_side, params) do
      [{binding, field, value} | filters]
    else
      _ -> filters
    end
  end

  defp maybe_add_membership_filter(filters, field_side, value_side, params) do
    with {binding, field} <- Upkeep.Ecto.Source.QueryDeps.Expressions.field_ref(field_side),
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

  defp put_fields(deps, filters) do
    Enum.reduce(filters, deps, fn {binding, field, _value}, deps ->
      put_field(deps, binding, field)
    end)
  end

  defp filters_by_schema(filters, deps) do
    filters
    |> Enum.group_by(fn {binding, _field, _value} -> Map.get(deps.bindings, binding) end)
    |> Map.delete(nil)
  end

  defp put_equality_filter_set(deps, _schema, filters) when map_size(filters) == 0, do: deps

  defp put_equality_filter_set(deps, schema, filters) do
    filter_sets =
      Map.update(deps.equality_filters, schema, [filters], &uniq_filters([filters | &1]))

    %{deps | equality_filters: filter_sets}
  end

  defp put_broad_reason(deps, schema, reason) when not is_nil(schema) do
    broad_reasons =
      Map.update(deps.broad_reasons, schema, MapSet.new([reason]), &MapSet.put(&1, reason))

    %{deps | broad?: true, broad_reasons: broad_reasons}
  end

  defp put_broad_reason(deps, _schema, _reason), do: deps

  defp mark_broad_for_unsupported(deps, query) do
    deps =
      query.wheres
      |> Enum.reduce(deps, fn where, deps ->
        params = params_by_index(where.params)
        mark_unsupported_or(deps, where.expr, params)
      end)

    query
    |> Upkeep.Ecto.Source.QueryDeps.Expressions.all()
    |> Enum.reduce(deps, &mark_fragments/2)
  end

  defp mark_unsupported_or(deps, {:or, _meta, [left, right]} = expr, params) do
    deps =
      with {:ok, [_ | _]} <- extract_equality_filter_sets(expr, params) do
        deps
      else
        _ ->
          expr
          |> schemas_for_expr(deps)
          |> Enum.reduce(deps, fn schema, deps ->
            put_broad_reason(deps, schema, :unsupported_or)
          end)
      end

    deps
    |> mark_unsupported_or(left, params)
    |> mark_unsupported_or(right, params)
  end

  defp mark_unsupported_or(deps, expr, params) do
    {_expr, deps} =
      Macro.prewalk(expr, deps, fn
        {:or, _meta, [_left, _right]} = expr, deps ->
          {expr, mark_unsupported_or(deps, expr, params)}

        node, deps ->
          {node, deps}
      end)

    deps
  end

  defp mark_fragments(expr, deps) do
    {_expr, deps} =
      Macro.prewalk(expr, deps, fn
        {:fragment, _meta, _parts} = fragment, deps ->
          deps =
            fragment
            |> schemas_for_expr(deps)
            |> Enum.reduce(deps, fn schema, deps -> put_broad_reason(deps, schema, :fragment) end)

          {fragment, deps}

        node, deps ->
          {node, deps}
      end)

    deps
  end

  defp schemas_for_expr(expr, deps) do
    {_expr, schemas} =
      Macro.prewalk(expr, MapSet.new(), fn node, schemas ->
        case Upkeep.Ecto.Source.QueryDeps.Expressions.field_ref(node) do
          {binding, _field} ->
            case Map.fetch(deps.bindings, binding) do
              {:ok, schema} -> {node, MapSet.put(schemas, schema)}
              :error -> {node, schemas}
            end

          nil ->
            {node, schemas}
        end
      end)

    case MapSet.to_list(schemas) do
      [] -> MapSet.to_list(deps.schemas)
      schemas -> schemas
    end
  end

  defp collect_subquery_deps(deps, query) do
    query
    |> Upkeep.Ecto.Source.QueryDeps.Expressions.structs()
    |> Enum.flat_map(&Map.get(&1, :subqueries, []))
    |> Enum.reduce(deps, fn %Ecto.SubQuery{query: query}, deps ->
      merge_deps(deps, from_query(query))
    end)
  end

  defp collect_preload_deps(deps, query) do
    preload_deps = preload_query_deps(query.preloads)
    schema_reasons = preload_schema_reasons(query)
    warnings = preload_warnings(query.preloads)

    deps =
      Enum.reduce(schema_reasons, deps, fn {schema, reason}, deps ->
        deps
        |> then(fn deps -> %{deps | schemas: MapSet.put(deps.schemas, schema)} end)
        |> put_broad_reason(schema, reason)
      end)

    deps
    |> merge_deps(preload_deps)
    |> then(fn deps -> %{deps | warnings: Enum.uniq(warnings ++ deps.warnings)} end)
  end

  defp preload_query_deps(preloads) when is_list(preloads) do
    preloads
    |> Enum.map(&preload_entry_deps/1)
    |> Enum.reduce(%__MODULE__{}, &merge_deps/2)
  end

  defp preload_query_deps(_preloads), do: %__MODULE__{}

  defp preload_entry_deps({_assoc, %Ecto.Query{} = query}), do: from_query(query)

  defp preload_entry_deps({_assoc, nested}) do
    nested
    |> List.wrap()
    |> preload_query_deps()
  end

  defp preload_entry_deps(%Ecto.Query{} = query), do: from_query(query)
  defp preload_entry_deps(_other), do: %__MODULE__{}

  defp preload_warnings(preloads) when is_list(preloads) do
    Enum.flat_map(preloads, fn
      {_assoc, %Ecto.Query{}} ->
        []

      {_assoc, fun} when is_function(fun) ->
        [%{reason: :unsupported_preload_function}]

      {_assoc, nested} ->
        preload_warnings(List.wrap(nested))

      %Ecto.Query{} ->
        []

      fun when is_function(fun) ->
        [%{reason: :unsupported_preload_function}]

      assoc when is_atom(assoc) ->
        []

      _other ->
        [%{reason: :unsupported_preload}]
    end)
  end

  defp preload_warnings(_preloads), do: []

  defp preload_schema_reasons(query) do
    root_schema = Map.get(Upkeep.Ecto.Source.QueryDeps.Bindings.from_query(query), 0)

    query.preloads
    |> schema_reasons_for_preloads(root_schema)
    |> Enum.concat(schema_reasons_for_assocs(query.assocs, query))
    |> Enum.uniq()
  end

  defp schema_reasons_for_preloads(_preloads, nil), do: []

  defp schema_reasons_for_preloads(preloads, owner_schema) when is_list(preloads) do
    Enum.flat_map(preloads, fn
      assoc when is_atom(assoc) ->
        association_schema_reasons(owner_schema, assoc)

      {assoc, %Ecto.Query{}} when is_atom(assoc) ->
        association_join_schema_reasons(owner_schema, assoc)

      {assoc, nested} when is_atom(assoc) ->
        nested_schemas =
          case association_schema_reasons(owner_schema, assoc) do
            [] -> []
            [{schema, _reason} | _] -> schema_reasons_for_preloads(List.wrap(nested), schema)
          end

        association_schema_reasons(owner_schema, assoc) ++ nested_schemas

      _other ->
        []
    end)
  end

  defp schema_reasons_for_preloads(_preloads, _owner_schema), do: []

  defp schema_reasons_for_assocs(assocs, query) when is_list(assocs) do
    bindings = Upkeep.Ecto.Source.QueryDeps.Bindings.from_query(query)

    Enum.flat_map(assocs, fn
      {assoc, {binding, nested}} when is_atom(assoc) and is_integer(binding) ->
        case Map.fetch(bindings, binding) do
          {:ok, schema} ->
            [{schema, :preload} | schema_reasons_for_preloads(List.wrap(nested), schema)]

          :error ->
            []
        end

      _other ->
        []
    end)
  end

  defp schema_reasons_for_assocs(_assocs, _query), do: []

  defp association_schema_reasons(owner_schema, assoc)
       when is_atom(owner_schema) and is_atom(assoc) do
    with true <- function_exported?(owner_schema, :__schema__, 2),
         association when not is_nil(association) <- owner_schema.__schema__(:association, assoc) do
      [
        {Map.get(association, :related), :preload},
        {Map.get(association, :join_through), :many_to_many_join}
      ]
      |> Enum.filter(fn {schema, _reason} ->
        (is_atom(schema) and not is_nil(schema)) or is_binary(schema)
      end)
    else
      _ -> []
    end
  rescue
    _ -> []
  end

  defp association_join_schema_reasons(owner_schema, assoc)
       when is_atom(owner_schema) and is_atom(assoc) do
    with true <- function_exported?(owner_schema, :__schema__, 2),
         association when not is_nil(association) <- owner_schema.__schema__(:association, assoc) do
      association
      |> Map.get(:join_through)
      |> then(fn
        value when (is_atom(value) and not is_nil(value)) or is_binary(value) ->
          [{value, :many_to_many_join}]

        _other ->
          []
      end)
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
        broad_reasons: merge_broad_reasons(left.broad_reasons, right.broad_reasons),
        broad?: left.broad? or right.broad?,
        warnings: Enum.uniq(left.warnings ++ right.warnings)
    }
  end

  defp append_precise(%Upkeep.Source.Coverage{} = coverage, schema, fields) do
    entry = %{schema: schema, fields: Enum.sort(fields)}
    %Upkeep.Source.Coverage{coverage | precise: [entry | coverage.precise]}
  end

  defp append_broad(%Upkeep.Source.Coverage{} = coverage, schema, reason) do
    entry = %{schema: schema, reason: reason}
    %Upkeep.Source.Coverage{coverage | broad: [entry | coverage.broad]}
  end

  defp unknown_warning?(%{reason: reason})
       when reason in [
              :unsupported_preload,
              :unsupported_preload_function
            ],
       do: true

  defp unknown_warning?(_warning), do: false

  defp merge_mapsets(left, right) do
    Map.merge(left, right, fn _schema, left_fields, right_fields ->
      MapSet.union(left_fields, right_fields)
    end)
  end

  defp merge_filters(left, right) do
    Map.merge(left, right, fn _schema, left_sets, right_sets ->
      uniq_filters(left_sets ++ right_sets)
    end)
  end

  defp merge_broad_reasons(left, right) do
    Map.merge(left, right, fn _schema, left_reasons, right_reasons ->
      MapSet.union(left_reasons, right_reasons)
    end)
  end

  defp uniq_filters(filter_sets), do: Enum.uniq_by(filter_sets, &Enum.sort(Map.to_list(&1)))

  defp interest_values(deps, schema) do
    cond do
      broad_schema?(deps, schema) ->
        [:broad]

      equality_filter_sets(deps, schema) == [] ->
        [:broad]

      true ->
        equality_value_sets(deps, schema)
    end
  end

  defp broad_schema?(deps, schema), do: broad_reasons(deps, schema) != []

  defp broad_reasons(deps, schema) do
    deps.broad_reasons
    |> Map.get(schema, MapSet.new())
    |> MapSet.to_list()
    |> Enum.sort()
  end

  defp equality_filter_sets(deps, schema) do
    deps.equality_filters
    |> Map.get(schema, [])
  end

  defp equality_value_sets(deps, schema) do
    deps
    |> equality_filter_sets(schema)
    |> Enum.flat_map(&filter_value_sets/1)
    |> Enum.uniq()
  end

  defp filter_value_sets(filters) when map_size(filters) == 0, do: []

  defp filter_value_sets(filters) do
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

  defp filter_set_fields(filter_sets) do
    filter_sets
    |> Enum.flat_map(&Map.keys/1)
    |> Enum.uniq()
  end

  defp change_matches_filters?(change, filters) do
    change
    |> Upkeep.Change.field_sets()
    |> Enum.any?(fn fields ->
      Enum.all?(filters, fn {field, values} -> Enum.member?(values, Map.get(fields, field)) end)
    end)
  end

  defp module_label(nil), do: "unknown"

  defp module_label(module) when is_atom(module) do
    module
    |> Module.split()
    |> Enum.join(".")
  rescue
    _error -> inspect(module, inspect_opts())
  end

  defp module_label(module), do: inspect(module, inspect_opts())

  defp join_or_empty([]), do: "none"
  defp join_or_empty(values), do: Enum.join(values, ", ")

  defp inspect_opts, do: [limit: 6, printable_limit: 160, charlists: :as_lists]
end
