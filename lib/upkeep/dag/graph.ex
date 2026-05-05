defmodule Upkeep.DAG.Graph do
  @moduledoc """
  Pure topology for a dependency graph.

  Tracks nodes, deps, and dependents. Knows nothing about cached values or
  compute functions. All queries return `Plan` structs or id lists, never a
  mutated graph — callers compose with `Upkeep.DAG.Store` when they need values.
  """

  defstruct nodes: %{}, deps: %{}, dependents: %{}

  alias Upkeep.DAG.Plan

  def new, do: %__MODULE__{}

  def has_node?(%__MODULE__{} = graph, id), do: Map.has_key?(graph.nodes, id)

  def kind(%__MODULE__{} = graph, id) do
    Map.fetch!(graph.nodes, id).kind
  end

  def deps(%__MODULE__{} = graph, id), do: Map.fetch!(graph.deps, id)

  def put_node(%__MODULE__{} = graph, id, kind, deps) when is_list(deps) do
    if same_topology?(graph, id, kind, deps) do
      graph
    else
      missing = Enum.reject(deps, &Map.has_key?(graph.nodes, &1))

      unless missing == [] do
        raise ArgumentError, "unknown DAG dependencies for #{inspect(id)}: #{inspect(missing)}"
      end

      graph
      |> insert_node(id, kind, deps)
      |> ensure_acyclic!()
    end
  end

  def remove_node(%__MODULE__{} = graph, id) do
    deps = Map.get(graph.deps, id, [])

    dependents =
      deps
      |> Enum.reduce(graph.dependents, fn dep, dependents ->
        Map.update(dependents, dep, MapSet.new(), &MapSet.delete(&1, id))
      end)
      |> Map.delete(id)

    %{
      graph
      | nodes: Map.delete(graph.nodes, id),
        deps: Map.delete(graph.deps, id),
        dependents: dependents
    }
  end

  def topological_order!(%__MODULE__{} = graph) do
    indegrees =
      Map.new(graph.nodes, fn {id, _node} -> {id, length(Map.get(graph.deps, id, []))} end)

    queue =
      indegrees
      |> Enum.filter(fn {_id, degree} -> degree == 0 end)
      |> Enum.map(fn {id, _degree} -> id end)
      |> sort_terms()

    order = kahn(graph, queue, indegrees, [])

    if length(order) == map_size(graph.nodes) do
      order
    else
      raise ArgumentError, "cycle detected in Upkeep DAG"
    end
  end

  def downstream_ids(%__MODULE__{} = graph, root_id) do
    downstream_order(graph, MapSet.new([root_id]))
  end

  def affected_ids(%__MODULE__{} = graph, root_ids) when is_list(root_ids) do
    downstream_order(graph, MapSet.new(root_ids))
  end

  def subgraph_plan(%__MODULE__{} = graph, root_id) do
    node_ids =
      if Map.has_key?(graph.nodes, root_id) do
        topological_subset(graph, MapSet.new([root_id | downstream_ids(graph, root_id)]))
      else
        []
      end

    subgraphs = subgraphs_for(graph, MapSet.new(node_ids))

    %Plan{
      roots: [root_id],
      selected_node_ids: node_ids,
      subgraphs: subgraphs,
      largest_subgraphs: largest_subgraphs(subgraphs),
      boundaries: []
    }
  end

  def applicable_subgraphs(%__MODULE__{} = graph, root_ids, classify)
      when is_list(root_ids) and is_function(classify, 2) do
    roots = sort_terms(root_ids)
    upstream = upstream_ids(graph, roots)

    {included, boundaries} =
      Enum.reduce(upstream, {MapSet.new(), []}, fn id, {included, boundaries} ->
        node = Map.fetch!(graph.nodes, id)

        case classify.(id, node) do
          :include ->
            {MapSet.put(included, id), boundaries}

          {:include, _metadata} ->
            {MapSet.put(included, id), boundaries}

          :exclude ->
            {included, [%{node_id: id, reason: :excluded} | boundaries]}

          {:exclude, reason} ->
            {included, [%{node_id: id, reason: reason} | boundaries]}
        end
      end)

    subgraphs = subgraphs_for(graph, included)

    %Plan{
      roots: roots,
      selected_node_ids: topological_subset(graph, included),
      subgraphs: subgraphs,
      largest_subgraphs: largest_subgraphs(subgraphs),
      boundaries: boundaries |> Enum.reverse() |> Enum.sort_by(&inspect(&1.node_id))
    }
  end

  def topological_subset(%__MODULE__{} = graph, ids) do
    included = MapSet.new(ids)

    graph
    |> topological_order!()
    |> Enum.filter(&MapSet.member?(included, &1))
  end

  def subgraphs_for(%__MODULE__{} = graph, included) do
    included
    |> connected_subgraphs(graph)
    |> Enum.map(fn node_ids ->
      ordered = topological_subset(graph, node_ids)
      %{node_ids: ordered, count: length(ordered)}
    end)
    |> Enum.sort_by(fn subgraph -> {-subgraph.count, inspect(subgraph.node_ids)} end)
  end

  def largest_subgraphs([]), do: []

  def largest_subgraphs(subgraphs) do
    largest_count = subgraphs |> Enum.map(& &1.count) |> Enum.max()
    Enum.filter(subgraphs, &(&1.count == largest_count))
  end

  def snapshot(%__MODULE__{} = graph) do
    order = topological_order!(graph)

    nodes =
      Enum.map(order, fn id ->
        node = Map.fetch!(graph.nodes, id)

        %{
          id: id,
          kind: node.kind,
          deps: Map.get(graph.deps, id, []),
          dependents: graph.dependents |> Map.get(id, MapSet.new()) |> sort_terms()
        }
      end)

    %{
      nodes: nodes,
      edges: edges(graph, order),
      topological_order: order
    }
  end

  # True when the node already exists with the same kind and deps. Lets the
  # Store skip topology rebuild + cycle check on the steady-state path.
  defp same_topology?(graph, id, kind, deps) do
    case Map.get(graph.nodes, id) do
      %{kind: ^kind} -> Map.get(graph.deps, id) == deps
      _ -> false
    end
  end

  defp insert_node(graph, id, kind, deps) do
    old_deps = Map.get(graph.deps, id, [])

    dependents =
      Enum.reduce(old_deps, graph.dependents, fn dep, dependents ->
        Map.update(dependents, dep, MapSet.new(), &MapSet.delete(&1, id))
      end)

    dependents =
      Enum.reduce(deps, dependents, fn dep, dependents ->
        Map.update(dependents, dep, MapSet.new([id]), &MapSet.put(&1, id))
      end)

    %{
      graph
      | nodes: Map.put(graph.nodes, id, %{id: id, kind: kind}),
        deps: Map.put(graph.deps, id, deps),
        dependents: Map.put_new(dependents, id, MapSet.new())
    }
  end

  defp downstream_order(graph, root_ids) do
    affected =
      root_ids
      |> Enum.flat_map(&downstream_ids_depth_first(graph, &1, MapSet.new()))
      |> MapSet.new()

    graph
    |> topological_order!()
    |> Enum.filter(&MapSet.member?(affected, &1))
  end

  defp downstream_ids_depth_first(graph, id, seen) do
    graph.dependents
    |> Map.get(id, MapSet.new())
    |> Enum.reject(&MapSet.member?(seen, &1))
    |> Enum.flat_map(fn dependent ->
      [dependent | downstream_ids_depth_first(graph, dependent, MapSet.put(seen, dependent))]
    end)
  end

  defp upstream_ids(graph, roots) do
    roots
    |> Enum.filter(&Map.has_key?(graph.nodes, &1))
    |> Enum.flat_map(&upstream_ids_depth_first(graph, &1, MapSet.new()))
    |> MapSet.new()
  end

  defp upstream_ids_depth_first(graph, id, seen) do
    if MapSet.member?(seen, id) do
      []
    else
      seen = MapSet.put(seen, id)

      deps =
        graph.deps
        |> Map.get(id, [])
        |> Enum.flat_map(&upstream_ids_depth_first(graph, &1, seen))

      [id | deps]
    end
  end

  defp connected_subgraphs(included, graph) do
    included
    |> Enum.reduce({[], included}, fn id, {components, unseen} ->
      if MapSet.member?(unseen, id) do
        component = connected_component(graph, id, included, MapSet.new())
        {[component | components], MapSet.difference(unseen, component)}
      else
        {components, unseen}
      end
    end)
    |> elem(0)
  end

  defp connected_component(graph, id, allowed, seen) do
    if MapSet.member?(seen, id) do
      seen
    else
      neighbors =
        graph.deps
        |> Map.get(id, [])
        |> Enum.concat(graph.dependents |> Map.get(id, MapSet.new()) |> MapSet.to_list())
        |> Enum.filter(&MapSet.member?(allowed, &1))

      Enum.reduce(neighbors, MapSet.put(seen, id), fn neighbor, seen ->
        connected_component(graph, neighbor, allowed, seen)
      end)
    end
  end

  defp edges(graph, order) do
    order
    |> Enum.flat_map(fn id ->
      graph.deps
      |> Map.get(id, [])
      |> Enum.map(fn dep -> %{from: dep, to: id} end)
    end)
  end

  defp ensure_acyclic!(graph) do
    topological_order!(graph)
    graph
  end

  defp kahn(_graph, [], _indegrees, order), do: Enum.reverse(order)

  defp kahn(graph, [id | queue], indegrees, order) do
    {queue, indegrees} =
      graph.dependents
      |> Map.get(id, MapSet.new())
      |> Enum.reduce({queue, indegrees}, fn dependent, {queue, indegrees} ->
        indegrees = Map.update!(indegrees, dependent, &(&1 - 1))

        if Map.fetch!(indegrees, dependent) == 0 do
          {sort_terms([dependent | queue]), indegrees}
        else
          {queue, indegrees}
        end
      end)

    kahn(graph, queue, indegrees, [id | order])
  end

  defp sort_terms(terms), do: Enum.sort_by(terms, &inspect/1)
end
