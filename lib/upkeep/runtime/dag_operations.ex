defmodule Upkeep.Runtime.DAGOperations do
  @moduledoc false

  alias Upkeep.Live.{Assigns, Components, Ids, Telemetry}
  alias Upkeep.Runtime.State

  def put_source(socket, source_id, value, deps) do
    {dag, _changed?} =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_source(Ids.source_node_id(source_id), value, deps)

    State.put_dag(socket, dag)
  end

  def put_value(socket, source_id, value, deps) do
    {dag, changed?} =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_source(Ids.source_node_id(source_id), value, deps)

    {State.put_dag(socket, dag), changed?}
  end

  def put_derived_value(socket, node_id, value) do
    {dag, changed?} =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_existing_value(node_id, value)

    {State.put_dag(socket, dag), changed?}
  end

  def remove_source(socket, source_id) do
    source_node_id = Ids.source_node_id(source_id)

    removed_node_ids = [
      source_node_id | Upkeep.DAG.downstream_ids(State.dag(socket), source_node_id)
    ]

    socket =
      removed_node_ids
      |> Enum.flat_map(&State.assign_names_for_node(socket, &1))
      |> Enum.reduce(socket, &State.delete_assign_node(&2, &1))

    dag =
      socket
      |> State.dag()
      |> Upkeep.DAG.remove_subgraph(source_node_id)

    State.put_dag(socket, dag)
  end

  def dependency_nodes(socket, deps) do
    deps
    |> Enum.map(fn dep ->
      node_id =
        Map.get(State.assign_nodes(socket), dep) ||
          raise ArgumentError, "unknown Upkeep dependency assign #{inspect(dep)}"

      {node_id, {dep, node_id}}
    end)
    |> Enum.unzip()
  end

  def recompute_derived(socket, []), do: socket

  def recompute_derived(socket, changed_source_nodes, remove_watch, opts \\ [])
      when is_function(remove_watch, 2) do
    skip_node_ids = Keyword.get(opts, :skip, [])

    {dag, changed_derived_nodes, _recomputed_nodes} =
      Telemetry.span([:dag, :recompute], %{changed_source_nodes: changed_source_nodes}, fn ->
        socket
        |> State.dag()
        |> Upkeep.DAG.recompute(changed_source_nodes, skip: skip_node_ids)
        |> then(fn {_dag, changed_derived_nodes, recomputed_nodes} = result ->
          {result,
           %{
             changed_derived_nodes: changed_derived_nodes,
             recomputed_nodes: recomputed_nodes,
             changed_count: length(changed_derived_nodes),
             recomputed_count: length(recomputed_nodes),
             skipped_nodes: skip_node_ids
           }}
        end)
      end)

    socket
    |> State.put_dag(dag)
    |> remove_changed_component_watches(changed_derived_nodes, remove_watch)
    |> assign_derived_nodes(changed_derived_nodes)
  end

  defp remove_changed_component_watches(socket, node_ids, remove_watch) do
    node_ids
    |> Components.changed_component_ids()
    |> Enum.reduce(socket, fn component_id, socket ->
      socket
      |> State.watches()
      |> Enum.filter(fn {_source_id, watch} -> watch.component == component_id end)
      |> Enum.reduce(socket, fn {source_id, _watch}, socket ->
        remove_watch.(socket, source_id)
      end)
    end)
  end

  defp assign_derived_nodes(socket, node_ids) do
    Enum.reduce(node_ids, socket, fn node_id, socket ->
      value = Upkeep.DAG.fetch!(State.dag(socket), node_id)
      assign_node_value(socket, node_id, value)
    end)
  end

  defp assign_node_value(socket, {:component, _component_id} = node_id, value) do
    Assigns.assign_component_value(socket, node_id, value)
  end

  defp assign_node_value(socket, node_id, value) do
    socket
    |> State.assign_names_for_node(node_id)
    |> Enum.reduce(socket, fn assign_name, socket ->
      Assigns.assign_derived_value(socket, assign_name, value, node_id)
    end)
  end
end
