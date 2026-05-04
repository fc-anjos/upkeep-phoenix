defmodule Upkeep.DAG do
  @moduledoc """
  Local dependency graph for source and derived values.

  The graph is deliberately data-only. It tracks nodes, edges, cached values,
  and recomputation order; callers decide how those values map back to LiveView
  assigns.
  """

  defstruct nodes: %{}, deps: %{}, dependents: %{}, values: %{}

  def new, do: %__MODULE__{}

  def put_source(%__MODULE__{} = dag, id, value, deps \\ []) when is_list(deps) do
    if same_topology?(dag, id, :source, deps) do
      put_value(dag, id, value)
    else
      missing = Enum.reject(deps, &Map.has_key?(dag.nodes, &1))

      unless missing == [] do
        raise ArgumentError, "unknown DAG dependencies for #{inspect(id)}: #{inspect(missing)}"
      end

      dag
      |> put_node(id, :source, deps, nil)
      |> ensure_acyclic!()
      |> put_value(id, value)
    end
  end

  def put_component(%__MODULE__{} = dag, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    missing = Enum.reject(deps, &Map.has_key?(dag.nodes, &1))

    unless missing == [] do
      raise ArgumentError, "unknown DAG dependencies for #{inspect(id)}: #{inspect(missing)}"
    end

    dag =
      dag
      |> put_node(id, :component, deps, compute)
      |> ensure_acyclic!()

    value = compute_dep_values(dag, id)
    {dag, _changed?} = put_value(dag, id, value)

    dag
  end

  def put_derived(dag, id, deps, compute, opts \\ [])

  def put_derived(%__MODULE__{} = dag, id, deps, compute, opts)
      when is_list(deps) and is_function(compute, 1) do
    if same_topology?(dag, id, :derived, deps) do
      # Topology unchanged; only the compute_fn or value can shift.
      dag = put_node(dag, id, :derived, deps, compute)

      value =
        case Keyword.fetch(opts, :initial_value) do
          {:ok, value} -> value
          :error -> compute_dep_values(dag, id)
        end

      {dag, _changed?} = put_value(dag, id, value)
      dag
    else
      missing = Enum.reject(deps, &Map.has_key?(dag.nodes, &1))

      unless missing == [] do
        raise ArgumentError, "unknown DAG dependencies for #{inspect(id)}: #{inspect(missing)}"
      end

      dag =
        dag
        |> put_node(id, :derived, deps, compute)
        |> ensure_acyclic!()

      value =
        case Keyword.fetch(opts, :initial_value) do
          {:ok, value} -> value
          :error -> compute_dep_values(dag, id)
        end

      {dag, _changed?} = put_value(dag, id, value)

      dag
    end
  end

  def fetch!(%__MODULE__{} = dag, id), do: Map.fetch!(dag.values, id)

  def has_node?(%__MODULE__{} = dag, id), do: Map.has_key?(dag.nodes, id)

  def recompute(%__MODULE__{} = dag, changed_ids) do
    changed_ids = MapSet.new(changed_ids)
    order = downstream_order(dag, changed_ids)

    {dag, changed, recomputed} =
      Enum.reduce(order, {dag, changed_ids, []}, fn id, {dag, changed, recomputed} ->
        deps = Map.fetch!(dag.deps, id)
        node = Map.fetch!(dag.nodes, id)

        if node.kind != :source and Enum.any?(deps, &MapSet.member?(changed, &1)) do
          value = compute_dep_values(dag, id)
          {dag, value_changed?} = put_value(dag, id, value)
          changed = if value_changed?, do: MapSet.put(changed, id), else: changed

          {dag, changed, [id | recomputed]}
        else
          {dag, changed, recomputed}
        end
      end)

    derived_changed =
      recomputed
      |> Enum.reverse()
      |> Enum.filter(&MapSet.member?(changed, &1))

    {dag, derived_changed, Enum.reverse(recomputed)}
  end

  def remove_subgraph(%__MODULE__{} = dag, root_id) do
    ids =
      [root_id | downstream_ids(dag, root_id)]
      |> MapSet.new()

    Enum.reduce(ids, dag, &remove_node(&2, &1))
  end

  def downstream_ids(%__MODULE__{} = dag, root_id) do
    dag
    |> downstream_order(MapSet.new([root_id]))
  end

  def affected_ids(%__MODULE__{} = dag, root_ids) when is_list(root_ids) do
    downstream_order(dag, MapSet.new(root_ids))
  end

  def snapshot(%__MODULE__{} = dag) do
    order = topological_order!(dag)

    nodes =
      Enum.map(order, fn id ->
        node = Map.fetch!(dag.nodes, id)

        %{
          id: id,
          kind: node.kind,
          deps: Map.get(dag.deps, id, []),
          dependents: dag.dependents |> Map.get(id, MapSet.new()) |> sort_terms(),
          has_value?: Map.has_key?(dag.values, id),
          value: Map.get(dag.values, id)
        }
      end)

    %{
      nodes: nodes,
      edges: edges(dag, order),
      topological_order: order
    }
  end

  # True when the node already exists with the same kind and deps. Lets
  # `put_source/4` and `put_derived/5` skip the topology rebuild + cycle
  # check on the steady-state value-update path.
  defp same_topology?(dag, id, kind, deps) do
    case Map.get(dag.nodes, id) do
      %{kind: ^kind} -> Map.get(dag.deps, id) == deps
      _ -> false
    end
  end

  defp put_node(dag, id, kind, deps, compute) do
    old_deps = Map.get(dag.deps, id, [])

    dependents =
      Enum.reduce(old_deps, dag.dependents, fn dep, dependents ->
        Map.update(dependents, dep, MapSet.new(), &MapSet.delete(&1, id))
      end)

    dependents =
      Enum.reduce(deps, dependents, fn dep, dependents ->
        Map.update(dependents, dep, MapSet.new([id]), &MapSet.put(&1, id))
      end)

    %{
      dag
      | nodes: Map.put(dag.nodes, id, %{id: id, kind: kind, compute: compute}),
        deps: Map.put(dag.deps, id, deps),
        dependents: Map.put_new(dependents, id, MapSet.new())
    }
  end

  defp put_value(dag, id, value) do
    old = Map.get(dag.values, id, :__upkeep_missing__)
    dag = %{dag | values: Map.put(dag.values, id, value)}

    {dag, old != value}
  end

  defp remove_node(dag, id) do
    deps = Map.get(dag.deps, id, [])

    dependents =
      deps
      |> Enum.reduce(dag.dependents, fn dep, dependents ->
        Map.update(dependents, dep, MapSet.new(), &MapSet.delete(&1, id))
      end)
      |> Map.delete(id)

    %{
      dag
      | nodes: Map.delete(dag.nodes, id),
        deps: Map.delete(dag.deps, id),
        dependents: dependents,
        values: Map.delete(dag.values, id)
    }
  end

  defp compute_dep_values(dag, id) do
    node = Map.fetch!(dag.nodes, id)

    dag.deps
    |> Map.fetch!(id)
    |> Map.new(fn dep -> {dep, Map.fetch!(dag.values, dep)} end)
    |> node.compute.()
  end

  defp downstream_order(dag, root_ids) do
    affected =
      root_ids
      |> Enum.flat_map(&downstream_ids_depth_first(dag, &1, MapSet.new()))
      |> MapSet.new()

    dag
    |> topological_order!()
    |> Enum.filter(&MapSet.member?(affected, &1))
  end

  defp downstream_ids_depth_first(dag, id, seen) do
    dag.dependents
    |> Map.get(id, MapSet.new())
    |> Enum.reject(&MapSet.member?(seen, &1))
    |> Enum.flat_map(fn dependent ->
      [dependent | downstream_ids_depth_first(dag, dependent, MapSet.put(seen, dependent))]
    end)
  end

  defp edges(dag, order) do
    order
    |> Enum.flat_map(fn id ->
      dag.deps
      |> Map.get(id, [])
      |> Enum.map(fn dep -> %{from: dep, to: id} end)
    end)
  end

  defp ensure_acyclic!(dag) do
    topological_order!(dag)
    dag
  end

  defp topological_order!(dag) do
    indegrees = Map.new(dag.nodes, fn {id, _node} -> {id, length(Map.get(dag.deps, id, []))} end)

    queue =
      indegrees
      |> Enum.filter(fn {_id, degree} -> degree == 0 end)
      |> Enum.map(fn {id, _degree} -> id end)
      |> sort_terms()

    order = kahn(dag, queue, indegrees, [])

    if length(order) == map_size(dag.nodes) do
      order
    else
      raise ArgumentError, "cycle detected in Upkeep DAG"
    end
  end

  defp kahn(_dag, [], _indegrees, order), do: Enum.reverse(order)

  defp kahn(dag, [id | queue], indegrees, order) do
    {queue, indegrees} =
      dag.dependents
      |> Map.get(id, MapSet.new())
      |> Enum.reduce({queue, indegrees}, fn dependent, {queue, indegrees} ->
        indegrees = Map.update!(indegrees, dependent, &(&1 - 1))

        if Map.fetch!(indegrees, dependent) == 0 do
          {sort_terms([dependent | queue]), indegrees}
        else
          {queue, indegrees}
        end
      end)

    kahn(dag, queue, indegrees, [id | order])
  end

  defp sort_terms(terms), do: Enum.sort_by(terms, &inspect/1)
end
