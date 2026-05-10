defmodule Upkeep.Coordinator.Graph.Shard.Flush do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Shard.{Dispatch, Loaders, Retries}
  alias Upkeep.Coordinator.LoadFailure
  alias Upkeep.Coordinator.LoadedSource
  alias Upkeep.Coordinator.Shards
  alias Upkeep.DAG.Store
  alias Upkeep.Coordinator.DirtyBuffer

  @flush_interval_ms 1

  def enqueue(state, node_ids) do
    state = Retries.reset(state, node_ids)
    enqueue_dirty(state, node_ids)
  end

  def enqueue_retry(state, node_ids) do
    enqueue_dirty(state, node_ids)
  end

  defp enqueue_dirty(state, node_ids) do
    case DirtyBuffer.enqueue(state.buffer, node_ids) do
      {:flush_now, buffer} ->
        flush(%{state | buffer: buffer})

      {:wait, buffer} ->
        %{state | buffer: buffer}

      {:schedule, buffer} ->
        Process.send_after(self(), :flush, @flush_interval_ms)
        %{state | buffer: buffer}
    end
  end

  def flush(state) do
    {dirty_ids, buffer} = DirtyBuffer.drain(state.buffer)
    state = %{state | buffer: buffer}

    case Enum.filter(dirty_ids, &Store.has_node?(state.store, &1)) do
      [] ->
        state

      dirty_sources ->
        run_flush(state, dirty_sources)
    end
  end

  defp run_flush(state, dirty_sources) do
    {sources_loaded, state} = load_sources(dirty_sources, state)

    {store, diff} = Store.recompute(state.store, Enum.map(sources_loaded, &elem(&1, 0)))

    derived_loaded =
      Enum.map(diff.changed_node_ids, fn id -> {id, Store.fetch!(store, id)} end)

    Dispatch.batch(state, sources_loaded ++ derived_loaded)

    %{state | store: store}
  end

  defp load_sources(node_ids, state) do
    stream =
      Task.Supervisor.async_stream_nolink(
        Shards.task_sup(),
        node_ids,
        fn node_id ->
          node = Store.fetch_metadata!(state.store, node_id)
          metadata = LoadedSource.load_metadata(state, node_id, node, :refresh)

          Loaders.run_with_deps(node_id, node, metadata)
        end,
        ordered: true,
        timeout: 30_000,
        on_timeout: :kill_task
      )

    {results, state} =
      node_ids
      |> Stream.zip(stream)
      |> Enum.reduce({[], state}, fn
        {_node_id, {:ok, %LoadedSource{node_id: node_id} = loaded}}, {results, state} ->
          state = Retries.clear(state, node_id)
          state = LoadedSource.apply(state, loaded)

          {[LoadedSource.pair(loaded) | results], state}

        {node_id, {:exit, reason}}, {results, state} ->
          node = Store.fetch_metadata!(state.store, node_id)
          {state, retry_metadata} = Retries.after_failure(state, node_id, node)
          failure = LoadFailure.new(node_id, node, reason, :refresh, retry_metadata)
          LoadFailure.emit(state, failure)

          {results, state}
      end)

    {Enum.reverse(results), state}
  end
end
