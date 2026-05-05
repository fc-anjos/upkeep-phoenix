defmodule Upkeep.Coordinator.Graph.Shard.Flush do
  @moduledoc false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Index
  alias Upkeep.Coordinator.Graph.Shard.{Dispatch, Loaders, Retries}
  alias Upkeep.Coordinator.Node
  alias Upkeep.DAG.Store

  @flush_interval_ms 1
  @flush_threshold 1_000

  def enqueue(state, node_ids) do
    state = Retries.reset(state, node_ids)
    enqueue_dirty(state, node_ids)
  end

  def enqueue_retry(state, node_ids) do
    enqueue_dirty(state, node_ids)
  end

  defp enqueue_dirty(state, node_ids) do
    new_buffer = Enum.reduce(node_ids, state.buffer_node_ids, &MapSet.put(&2, &1))
    state = %{state | buffer_node_ids: new_buffer, buffer_size: MapSet.size(new_buffer)}

    cond do
      state.buffer_size >= @flush_threshold ->
        flush(state)

      state.flush_scheduled? ->
        state

      true ->
        Process.send_after(self(), :flush, @flush_interval_ms)
        %{state | flush_scheduled?: true}
    end
  end

  def flush(%{buffer_size: 0} = state), do: %{state | flush_scheduled?: false}

  def flush(state) do
    dirty_sources =
      state.buffer_node_ids
      |> MapSet.to_list()
      |> Enum.filter(&Map.has_key?(state.sources, &1))

    {sources_loaded, state} = load_sources(dirty_sources, state)

    store =
      Enum.reduce(sources_loaded, state.store, fn {id, value}, store ->
        {store, _changed?} = Store.put_source(store, id, value, [])
        store
      end)

    {store, diff} = Store.recompute(store, Enum.map(sources_loaded, &elem(&1, 0)))

    derived_loaded =
      Enum.map(diff.changed_node_ids, fn id -> {id, Store.fetch!(store, id)} end)

    Dispatch.batch(state, sources_loaded ++ derived_loaded)

    %{
      state
      | store: store,
        buffer_node_ids: MapSet.new(),
        buffer_size: 0,
        flush_scheduled?: false
    }
  end

  defp load_sources(node_ids, state) do
    stream =
      Task.Supervisor.async_stream_nolink(
        Graph.task_sup(),
        node_ids,
        fn node_id ->
          node = Map.fetch!(state.sources, node_id)

          {value, current_keys, tracked_deps} = Loaders.run_with_deps(node.loader)
          {node_id, value, current_keys, tracked_deps, node}
        end,
        ordered: true,
        timeout: 30_000,
        on_timeout: :kill_task
      )

    {results, state} =
      node_ids
      |> Stream.zip(stream)
      |> Enum.reduce({[], state}, fn
        {_node_id, {:ok, {node_id, value, current_keys, tracked_deps, %Node{} = node}}},
        {results, state} ->
          if current_keys != node.registered_keys do
            Index.reconcile_source(node_id, state.idx, node.registered_keys, current_keys)
          end

          state = Retries.clear(state, node_id)

          sources =
            Map.put(state.sources, node_id, %Node{
              node
              | registered_keys: current_keys,
                tracked_deps: tracked_deps,
                loaded?: true
            })

          {[{node_id, value} | results], %{state | sources: sources}}

        {node_id, {:exit, reason}}, {results, state} ->
          node = Map.fetch!(state.sources, node_id)
          {state, retry_metadata} = Retries.after_failure(state, node_id, node)

          :telemetry.execute(
            [:upkeep, :graph, :source_load, :exception],
            %{count: 1},
            node
            |> exception_metadata(state, reason)
            |> Map.merge(retry_metadata)
            |> Map.put(:node_id, node_id)
          )

          {results, state}
      end)

    {Enum.reverse(results), state}
  end

  defp exception_metadata(%Node{} = node, state, reason) do
    node.loader
    |> Loaders.exception_metadata(reason)
    |> Map.put(:shard, state.idx)
    |> Map.put(:subscriber_count, subscriber_count(node))
  end

  defp subscriber_count(%Node{encoded_key: encoded_key}) do
    Group.member_count(Graph.group(), encoded_key)
  end
end
