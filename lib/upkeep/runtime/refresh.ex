defmodule Upkeep.Runtime.Refresh do
  @moduledoc false

  alias Upkeep.Runtime.Patch
  alias Upkeep.Runtime.SourceLoads
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Subscriptions
  alias Upkeep.Runtime.Telemetry
  alias Upkeep.Runtime.Watches
  alias Upkeep.Source.Loader, as: Source

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    result = Source.load_result(source, params)

    {:ok, socket, [{:assign, assign_name, result.value}]}
  end

  def refresh_matching(socket, event) when is_struct(event) do
    {:ok, socket, queue_effects} = queue_matching(socket, event)
    {:ok, socket, flush_effects} = flush_refreshes(socket)
    {:ok, socket, queue_effects ++ flush_effects}
  end

  def refresh_local_matching(socket, event) when is_struct(event) do
    {:ok, socket, queue_effects} = queue_matching(socket, event, :local)
    {:ok, socket, flush_effects} = flush_refreshes(socket)
    {:ok, socket, queue_effects ++ flush_effects}
  end

  def queue_matching(socket, event, mode \\ :all) when is_struct(event) do
    socket
    |> State.matching_watches(event)
    |> Enum.reduce({socket, []}, fn {_source_id, watch}, {socket, effects} ->
      if refresh_watch?(watch, event, mode) do
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

  defp refresh_watch?(watch, event, :all), do: SourceLoads.reacts_to?(watch, event)

  defp refresh_watch?(watch, event, :local) do
    SourceLoads.reacts_to?(watch, event) and local_watch?(watch)
  end

  defp local_watch?(watch) do
    not Map.get(watch, :registered?, false) and
      not Subscriptions.registered_source?(watch.source_id)
  end

  def flush_refreshes(socket) do
    patch =
      socket
      |> State.pending_refreshes()
      |> Enum.reduce(Patch.new(State.clear_pending_refreshes(socket)), fn source_id, patch ->
        refresh_queued_source(patch, source_id)
      end)
      |> Patch.recompute(&Watches.remove_watch/2)

    Patch.result(patch)
  end

  defp refresh_queued_source(%Patch{} = patch, source_id) do
    with_watch(patch, source_id, fn watch ->
      maybe_refresh(patch, watch)
    end)
  end

  defp with_watch(%Patch{} = patch, source_id, found) do
    case Map.fetch(State.watches(Patch.socket(patch)), source_id) do
      {:ok, watch} -> found.(watch)
      :error -> patch
    end
  end

  defp maybe_refresh(%Patch{} = patch, watch) do
    case SourceLoads.safe_load(watch, :refresh) do
      {:ok, result} ->
        watch = SourceLoads.update_watch_result(watch, result)

        patch
        |> Patch.replace_socket(
          State.put_existing_watch(Patch.socket(patch), watch.source_id, watch)
        )
        |> Patch.put_watch_value(watch, result.value)

      {:error, _info} ->
        # A failing local refresh must not crash the LiveView. Skip the update
        # and keep the prior assign value; the `[:upkeep, :source, :reload]`
        # span already emitted an `:exception` event for observability.
        patch
    end
  end
end
