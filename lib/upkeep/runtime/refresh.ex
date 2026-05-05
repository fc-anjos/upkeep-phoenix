defmodule Upkeep.Runtime.Refresh do
  @moduledoc false

  alias Upkeep.Live.{Ids, Telemetry}
  alias Upkeep.Runtime.DAGOperations
  alias Upkeep.Runtime.Effects
  alias Upkeep.Runtime.SourceLoads
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Watches
  alias Upkeep.Source.Loader, as: Source

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    {value, _tracked_deps} = Source.load(source, params)
    {:ok, socket, [{:assign, assign_name, value}]}
  end

  def refresh_matching(socket, event) when is_struct(event) do
    {:ok, socket, queue_effects} = queue_matching(socket, event)
    {:ok, socket, flush_effects} = flush_refreshes(socket)
    {:ok, socket, queue_effects ++ flush_effects}
  end

  def queue_matching(socket, event) when is_struct(event) do
    socket
    |> State.watches()
    |> Enum.reduce({socket, []}, fn {_source_id, watch}, {socket, effects} ->
      if SourceLoads.reacts_to?(watch, event) do
        socket = State.queue_refresh(socket, watch.source_id)

        effects = [
          {:telemetry, [:source, :queue], %{count: 1},
           Telemetry.watch_metadata(watch, event: event)}
          | effects
        ]

        {socket, effects}
      else
        {socket, effects}
      end
    end)
    |> then(fn {socket, effects} -> {:ok, socket, Enum.reverse(effects)} end)
  end

  def flush_refreshes(socket) do
    {socket, changed_source_nodes, effects} =
      socket
      |> State.pending_refreshes()
      |> Enum.reduce({State.clear_pending_refreshes(socket), [], []}, fn source_id,
                                                                         {socket, changed,
                                                                          effects} ->
        refresh_queued_source(socket, source_id, changed)
        |> then(fn {socket, changed, refresh_effects} ->
          {socket, changed, effects ++ refresh_effects}
        end)
      end)

    {socket, recompute_effects} = recompute_derived(socket, changed_source_nodes)
    {:ok, socket, effects ++ recompute_effects}
  end

  defp refresh_queued_source(socket, source_id, changed) do
    with_watch(socket, source_id, {socket, changed, []}, fn watch ->
      maybe_refresh(socket, watch, changed)
    end)
  end

  defp with_watch(socket, source_id, fallback, found) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} -> found.(watch)
      :error -> fallback
    end
  end

  defp maybe_refresh(socket, watch, changed) do
    {value, tracked_deps} = SourceLoads.load(watch, :refresh)
    watch = SourceLoads.update_watch_deps(watch, tracked_deps)
    socket = State.put_existing_watch(socket, watch.source_id, watch)

    {socket, changed?} =
      DAGOperations.put_value(socket, watch.source_id, value, Ids.source_deps(watch.component))

    changed =
      if changed? do
        [Ids.source_node_id(watch.source_id) | changed]
      else
        changed
      end

    {socket, changed, Effects.assign_watch(watch, value)}
  rescue
    _ -> {socket, changed, []}
  end

  defp recompute_derived(socket, []), do: {socket, []}

  defp recompute_derived(socket, changed_source_nodes) do
    DAGOperations.recompute_derived(socket, changed_source_nodes, &Watches.remove_watch/2)
  end
end
