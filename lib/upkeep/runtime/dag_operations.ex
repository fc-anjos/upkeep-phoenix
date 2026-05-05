defmodule Upkeep.Runtime.DAGOperations do
  @moduledoc false

  alias Upkeep.DAG.{Graph, Store}
  alias Upkeep.Live.{Components, Ids, Telemetry}
  alias Upkeep.Runtime.State

  def put_source(socket, source_id, value, deps) do
    {store, _changed?} =
      socket
      |> State.store()
      |> Store.put_source(Ids.source_node_id(source_id), value, deps)

    State.put_store(socket, store)
  end

  def put_value(socket, source_id, value, deps) do
    {store, changed?} =
      socket
      |> State.store()
      |> Store.put_source(Ids.source_node_id(source_id), value, deps)

    {State.put_store(socket, store), changed?}
  end

  def put_derived_value(socket, node_id, value) do
    {store, changed?} =
      socket
      |> State.store()
      |> Store.seed(node_id, value)

    {State.put_store(socket, store), changed?}
  end

  def remove_source(socket, source_id) do
    source_node_id = Ids.source_node_id(source_id)
    store = State.store(socket)
    remove_plan = Graph.subgraph_plan(Store.graph(store), source_node_id)
    removed_node_ids = remove_plan.selected_node_ids

    socket =
      removed_node_ids
      |> Enum.flat_map(&State.assign_names_for_node(socket, &1))
      |> Enum.reduce(socket, &State.delete_assign_node(&2, &1))

    State.put_store(socket, Store.remove_subgraph(store, source_node_id))
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

  def recompute_derived(socket, []), do: {socket, []}

  def recompute_derived(socket, changed_source_nodes, remove_watch, opts \\ [])
      when is_function(remove_watch, 2) do
    skip_node_ids = Keyword.get(opts, :skip, [])

    {store, diff} =
      Telemetry.span([:dag, :recompute], %{changed_source_nodes: changed_source_nodes}, fn ->
        socket
        |> State.store()
        |> Store.recompute(changed_source_nodes, skip: skip_node_ids)
        |> then(fn {_store, diff} = result ->
          {result,
           %{
             affected_nodes: diff.selected_node_ids,
             changed_derived_nodes: diff.changed_node_ids,
             recomputed_nodes: diff.recomputed_node_ids,
             changed_count: length(diff.changed_node_ids),
             recomputed_count: length(diff.recomputed_node_ids),
             skipped_nodes: diff.skipped_node_ids
           }}
        end)
      end)

    socket
    |> State.put_store(store)
    |> remove_changed_component_watches(diff.changed_node_ids, remove_watch)
    |> then(fn {socket, remove_effects} ->
      {socket, assign_effects} = assign_derived_nodes(socket, diff.changed_node_ids)
      {socket, remove_effects ++ assign_effects}
    end)
  end

  defp remove_changed_component_watches(socket, node_ids, remove_watch) do
    node_ids
    |> Components.changed_component_ids()
    |> Enum.reduce({socket, []}, fn component_id, {socket, effects} ->
      socket
      |> State.watches()
      |> Enum.filter(fn {_source_id, watch} -> watch.component == component_id end)
      |> Enum.reduce({socket, effects}, fn {source_id, _watch}, {socket, effects} ->
        {socket, remove_effects} = remove_watch.(socket, source_id)
        {socket, effects ++ remove_effects}
      end)
    end)
  end

  defp assign_derived_nodes(socket, node_ids) do
    Enum.reduce(node_ids, {socket, []}, fn node_id, {socket, effects} ->
      value = Store.fetch!(State.store(socket), node_id)
      {socket, effects ++ assign_node_effects(socket, node_id, value)}
    end)
  end

  defp assign_node_effects(_socket, {:component, _component_id}, value) when is_map(value) do
    value
    |> Enum.flat_map(fn
      {assign_name, assign_value} when is_atom(assign_name) ->
        [{:assign, assign_name, assign_value}]

      {_assign_name, _assign_value} ->
        []
    end)
  end

  defp assign_node_effects(_socket, {:component, _component_id}, _value), do: []

  defp assign_node_effects(socket, node_id, value) do
    socket
    |> State.assign_names_for_node(node_id)
    |> Enum.flat_map(fn assign_name ->
      [
        {:telemetry, [:live, :assign], %{count: 1},
         %{assign: assign_name, node_id: node_id, kind: :derived}},
        {:assign, assign_name, value}
      ]
    end)
  end
end
