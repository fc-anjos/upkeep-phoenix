defmodule Upkeep.Runtime.Watches do
  @moduledoc false

  alias Upkeep.Runtime.{Ids, Patch, State, Subscriptions, Telemetry}

  def remove_component(socket, component_id) when not is_nil(component_id) do
    node_id = Ids.component_node_id(component_id)
    removed_node_ids = Patch.subgraph_node_ids(socket, node_id)

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

    patch =
      socket
      |> Patch.new()
      |> Patch.remove_subgraph(node_id)

    {:ok, Patch.socket(patch), effects ++ Patch.effects(patch)}
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
      watch.instance.source == source and watch.instance.params == params
    end)
    |> Enum.reduce({socket, []}, fn {source_id, _watch}, {socket, effects} ->
      {socket, remove_effects} = remove_watch(socket, source_id)
      {socket, effects ++ remove_effects}
    end)
    |> then(fn {socket, effects} -> {:ok, socket, effects} end)
  end

  def revoke(socket, predicate) when is_function(predicate, 1) do
    socket
    |> State.watches()
    |> Enum.filter(fn {_source_id, watch} ->
      predicate.(Map.get(watch, :authorizing_identity))
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
          |> maybe_leave_local_notifications(watches)

        Upkeep.Invalidation.release_read_holder(source_id)

        patch =
          socket
          |> Patch.new()
          |> Patch.remove_subgraph(Ids.source_node_id(source_id))

        effects =
          Patch.effects(patch) ++
            maybe_unregister_effect(watch, source_id) ++
            [
              {:telemetry, [:source, :unwatch], %{count: 1}, Telemetry.watch_metadata(watch)}
            ]

        {Patch.socket(patch), effects}

      :error ->
        {socket, []}
    end
  end

  defp remove_watch_assign(socket, source_id, assign_name) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        remove_watch_assign(socket, source_id, watch, assign_name)

      :error ->
        {socket, []}
    end
  end

  defp remove_watch_assign(socket, source_id, watch, assign_name) do
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
            Telemetry.watch_alias_metadata(watch, assign_name)}
         ]}
      end)
    end
  end

  defp maybe_unregister_effect(%{registered?: true}, source_id), do: [{:unregister, source_id}]
  defp maybe_unregister_effect(%{subscribed?: true}, source_id), do: [{:unregister, source_id}]
  defp maybe_unregister_effect(_watch, _source_id), do: []

  defp maybe_leave_local_notifications(socket, watches) do
    if local_notifications_needed?(watches) do
      socket
    else
      :ok = Subscriptions.leave_local_notifications()
      socket
    end
  end

  defp local_notifications_needed?(watches) do
    Enum.any?(watches, fn {_source_id, watch} ->
      not Map.get(watch, :registered?, false) and
        not Subscriptions.registered_source?(watch.source_id)
    end)
  end
end
