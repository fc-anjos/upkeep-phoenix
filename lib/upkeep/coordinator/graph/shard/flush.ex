defmodule Upkeep.Coordinator.Graph.Shard.Flush do
  @moduledoc false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Index
  alias Upkeep.Coordinator.Graph.Shard.{Dispatch, Loaders}
  alias Upkeep.Coordinator.Node
  alias Upkeep.DAG

  @flush_interval_ms 1
  @flush_threshold 1_000

  def enqueue(state, node_ids) do
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

    dag =
      Enum.reduce(sources_loaded, state.dag, fn {id, value}, dag ->
        {dag, _changed?} = DAG.put_source(dag, id, value, [])
        dag
      end)

    {dag, derived_changed, _} = DAG.recompute(dag, Enum.map(sources_loaded, &elem(&1, 0)))

    derived_loaded = Enum.map(derived_changed, fn id -> {id, DAG.fetch!(dag, id)} end)

    Dispatch.batch(state, sources_loaded ++ derived_loaded)

    %{
      state
      | dag: dag,
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

    {results, sources} =
      node_ids
      |> Stream.zip(stream)
      |> Enum.reduce({[], state.sources}, fn
        {_node_id, {:ok, {node_id, value, current_keys, tracked_deps, %Node{} = node}}},
        {results, sources} ->
          if current_keys != node.registered_keys do
            Index.reconcile_source(node_id, state.idx, node.registered_keys, current_keys)
          end

          sources =
            Map.put(sources, node_id, %Node{
              node
              | registered_keys: current_keys,
                tracked_deps: tracked_deps,
                loaded?: true
            })

          {[{node_id, value} | results], sources}

        {node_id, {:exit, reason}}, {results, sources} ->
          node = Map.fetch!(state.sources, node_id)

          :telemetry.execute(
            [:upkeep, :graph, :source_load, :exception],
            %{count: 1},
            node
            |> exception_metadata(state, reason)
            |> Map.put(:node_id, node_id)
          )

          {results, sources}
      end)

    {Enum.reverse(results), %{state | sources: sources}}
  end

  defp exception_metadata(%Node{} = node, state, reason) do
    node.loader
    |> Loaders.exception_metadata(reason)
    |> Map.put(:shard, state.idx)
    |> Map.put(:subscriber_count, subscriber_count(node))
  end

  defp subscriber_count(%Node{encoded_key: encoded_key}) do
    Graph.group()
    |> Group.members(encoded_key)
    |> length()
  end
end
