defmodule Upkeep.Runtime.Watches do
  @moduledoc false

  alias Upkeep.Internal.DAG.{Graph, Store}
  alias Upkeep.Live.{Ids, Telemetry}
  alias Upkeep.Runtime.DAGOperations
  alias Upkeep.Runtime.State

  def remove_component(socket, component_id) when not is_nil(component_id) do
    node_id = Ids.component_node_id(component_id)
    remove_plan = Graph.subgraph_plan(Store.graph(State.store(socket)), node_id)
    removed_node_ids = remove_plan.selected_node_ids

    {socket, effects} =
      socket
      |> State.watches()
      |> Enum.filter(fn {_source_id, watch} ->
        Enum.member?(removed_node_ids, Ids.source_node_id(watch.source_id))
      end)
      |> Enum.reduce({socket, []}, fn {source_id, _watch}, {socket, effects} ->
        {socket, remove_effects} = remove_watch(socket, source_id)
        {socket, effects ++ remove_effects}
      end)

    store =
      socket
      |> State.store()
      |> Store.remove_subgraph(node_id)

    removed_node_ids
    |> Enum.flat_map(&State.assign_names_for_node(socket, &1))
    |> Enum.reduce(State.put_store(socket, store), &State.delete_assign_node(&2, &1))
    |> then(fn socket -> {:ok, socket, effects} end)
  end

  def unwatch_assign(socket, assign_name) when is_atom(assign_name) do
    socket
    |> State.watches()
    |> Enum.filter(fn {_source_id, watch} -> MapSet.member?(watch.assign_names, assign_name) end)
    |> Enum.reduce({socket, []}, fn {source_id, _watch}, {socket, effects} ->
      {socket, remove_effects} = remove_watch_assign(socket, source_id, assign_name)
      {socket, effects ++ remove_effects}
    end)
    |> then(fn {socket, effects} -> {:ok, socket, effects} end)
  end

  def unwatch_source(socket, source, params) when is_atom(source) do
    socket
    |> State.watches()
    |> Enum.filter(fn {_source_id, watch} ->
      watch.source == source and watch.params == params
    end)
    |> Enum.reduce({socket, []}, fn {source_id, _watch}, {socket, effects} ->
      {socket, remove_effects} = remove_watch(socket, source_id)
      {socket, effects ++ remove_effects}
    end)
    |> then(fn {socket, effects} -> {:ok, socket, effects} end)
  end

  def remove_watch(socket, source_id) do
    current_watches = State.watches(socket)

    case Map.fetch(current_watches, source_id) do
      {:ok, watch} ->
        watches = Map.delete(current_watches, source_id)

        socket =
          socket
          |> State.put_watches(watches)
          |> State.delete_pending_refresh(source_id)

        watch.assign_names
        |> Enum.reduce(socket, &State.delete_assign_node(&2, &1))
        |> DAGOperations.remove_source(source_id)
        |> then(fn socket ->
          effects =
            maybe_unregister_effect(watch.registered?, source_id) ++
              [
                {:telemetry, [:source, :unwatch], %{count: 1}, Telemetry.watch_metadata(watch)}
              ]

          {socket, effects}
        end)

      :error ->
        {socket, []}
    end
  end

  defp remove_watch_assign(socket, source_id, assign_name) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        assign_names = MapSet.delete(watch.assign_names, assign_name)

        if Enum.empty?(assign_names) do
          remove_watch(socket, source_id)
        else
          socket
          |> State.put_existing_watch(source_id, %{watch | assign_names: assign_names})
          |> State.delete_assign_node(assign_name)
          |> then(fn socket ->
            {socket,
             [
               {:telemetry, [:source, :unwatch], %{count: 1},
                Telemetry.watch_metadata(watch, assign_name, :alias)}
             ]}
          end)
        end

      :error ->
        {socket, []}
    end
  end

  defp maybe_unregister_effect(true, source_id), do: [{:unregister, source_id}]
  defp maybe_unregister_effect(false, _source_id), do: []
end
