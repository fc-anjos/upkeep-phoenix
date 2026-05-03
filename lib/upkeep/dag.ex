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
    missing = Enum.reject(deps, &Map.has_key?(dag.nodes, &1))

    unless missing == [] do
      raise ArgumentError, "unknown DAG dependencies for #{inspect(id)}: #{inspect(missing)}"
    end

    dag
    |> put_node(id, :source, deps, nil)
    |> ensure_acyclic!()
    |> put_value(id, value)
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

  def put_derived(%__MODULE__{} = dag, id, deps, compute)
      when is_list(deps) and is_function(compute, 1) do
    missing = Enum.reject(deps, &Map.has_key?(dag.nodes, &1))

    unless missing == [] do
      raise ArgumentError, "unknown DAG dependencies for #{inspect(id)}: #{inspect(missing)}"
    end

    dag =
      dag
      |> put_node(id, :derived, deps, compute)
      |> ensure_acyclic!()

    value = compute_dep_values(dag, id)
    {dag, _changed?} = put_value(dag, id, value)

    dag
  end

  def fetch!(%__MODULE__{} = dag, id), do: Map.fetch!(dag.values, id)

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
