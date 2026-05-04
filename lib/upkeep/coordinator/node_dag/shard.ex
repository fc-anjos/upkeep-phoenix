defmodule Upkeep.Coordinator.NodeDAG.Shard do
  @moduledoc false
  use GenServer

  alias Upkeep.Coordinator.NodeDAG
  alias Upkeep.DAG

  @flush_interval_ms 1
  @flush_threshold 1_000
  @generation_table :upkeep_node_dag_shard_generations

  ## Public

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    idx = Keyword.fetch!(opts, :idx)
    GenServer.start_link(__MODULE__, idx, name: name)
  end

  ## GenServer

  @impl true
  def init(idx) do
    sweep_owned_ets(idx)

    # Subscribe only to lifecycle events for source-key leaves; we use
    # `Group.member_count/2` on :left to decide whether to remove a node.
    # :joined events are no-ops (registration creates the node directly).
    :ok = Group.monitor(NodeDAG.group(), "node_dag/source/")

    generation = bump_generation(idx)

    :ok =
      Group.register(NodeDAG.group(), NodeDAG.shard_key(idx), %{
        pid: self(),
        generation: generation
      })

    {:ok,
     %{
       idx: idx,
       generation: generation,
       dag: DAG.new(),
       # node_id => {load_fn, registered_keys, encoded_key}
       sources: %{},
       buffer_node_ids: MapSet.new(),
       buffer_size: 0,
       flush_scheduled?: false
     }}
  end

  @impl true
  def handle_call({:register_source, node_id, interest_keys, load_fn}, _from, state) do
    encoded_key = NodeDAG.source_key(node_id)
    :ets.insert(NodeDAG.nodes_table(), {node_id, :source, state.idx, interest_keys})
    Enum.each(interest_keys, &:ets.insert(NodeDAG.index_table(), {&1, node_id}))
    {dag, _changed?} = DAG.put_source(state.dag, node_id, nil, [])

    state = %{
      state
      | sources: Map.put(state.sources, node_id, {load_fn, interest_keys, encoded_key}),
        dag: dag
    }

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:register_derived, node_id, dep_ids, compute_fn}, _from, state) do
    encoded_key = NodeDAG.source_key(node_id)
    :ets.insert(NodeDAG.nodes_table(), {node_id, :derived, state.idx, []})

    state = %{
      state
      | dag: DAG.put_derived(state.dag, node_id, dep_ids, compute_fn, initial_value: nil),
        sources: Map.put_new(state.sources, node_id, {nil, [], encoded_key})
    }

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:drain, _from, state), do: {:reply, :ok, do_flush(state)}

  @impl true
  def handle_call({:notify, _event, node_ids}, _from, state),
    do: {:reply, :ok, enqueue(state, node_ids)}

  @impl true
  def handle_cast({:notify, _event, node_ids}, state),
    do: {:noreply, enqueue(state, node_ids)}

  @impl true
  def handle_info(:flush, state), do: {:noreply, do_flush(state)}

  @impl true
  def handle_info({:group, events, _info}, state) do
    state = Enum.reduce(events, state, &handle_group_event/2)
    {:noreply, state}
  end

  defp handle_group_event(%{type: :left, key: key}, state) do
    case decode_owned_source_key(key, state.idx) do
      :other ->
        state

      node_id ->
        # Group has already removed the leaving pid. If no members remain,
        # the node is now untracked — remove it.
        if Group.member_count(NodeDAG.group(), key) == 0,
          do: remove_node(state, node_id),
          else: state
    end
  end

  defp handle_group_event(_other, state), do: state

  defp decode_owned_source_key(key, idx) do
    node_id = NodeDAG.decode_source_key(key)

    case :ets.lookup(NodeDAG.nodes_table(), node_id) do
      [{^node_id, _kind, ^idx, _keys}] -> node_id
      _ -> :other
    end
  rescue
    _ -> :other
  end

  defp remove_node(state, node_id) do
    case :ets.lookup(NodeDAG.nodes_table(), node_id) do
      [{^node_id, _kind, _idx, keys}] ->
        Enum.each(keys, &:ets.delete_object(NodeDAG.index_table(), {&1, node_id}))
        :ets.delete(NodeDAG.nodes_table(), node_id)

      _ ->
        :ok
    end

    %{
      state
      | sources: Map.delete(state.sources, node_id),
        dag:
          if(DAG.has_node?(state.dag, node_id),
            do: DAG.remove_subgraph(state.dag, node_id),
            else: state.dag
          )
    }
  end

  ## Buffer / flush

  defp enqueue(state, node_ids) do
    new_buffer = Enum.reduce(node_ids, state.buffer_node_ids, &MapSet.put(&2, &1))
    state = %{state | buffer_node_ids: new_buffer, buffer_size: MapSet.size(new_buffer)}

    cond do
      state.buffer_size >= @flush_threshold ->
        do_flush(state)

      state.flush_scheduled? ->
        state

      true ->
        Process.send_after(self(), :flush, @flush_interval_ms)
        %{state | flush_scheduled?: true}
    end
  end

  defp do_flush(%{buffer_size: 0} = state), do: %{state | flush_scheduled?: false}

  defp do_flush(state) do
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

    dispatch_batch(state, sources_loaded ++ derived_loaded)

    %{
      state
      | dag: dag,
        buffer_node_ids: MapSet.new(),
        buffer_size: 0,
        flush_scheduled?: false
    }
  end

  defp load_sources(node_ids, state) do
    # Two passes so tasks run in parallel: spawn all, then await all.
    pending =
      Enum.map(node_ids, fn node_id ->
        {load_fn, registered_keys, encoded_key} = Map.fetch!(state.sources, node_id)

        task =
          Task.Supervisor.async_nolink(NodeDAG.task_sup(), fn ->
            {value, current_keys} = load_fn.()
            {value, current_keys}
          end)

        {node_id, load_fn, registered_keys, encoded_key, task}
      end)

    {results, sources} =
      Enum.map_reduce(pending, state.sources, fn {node_id, load_fn, registered_keys, encoded_key,
                                                   task},
                                                  sources ->
        {value, current_keys} = Task.await(task, 30_000)

        sources =
          if current_keys != registered_keys do
            reconcile_index(node_id, registered_keys, current_keys, state.idx)
            Map.put(sources, node_id, {load_fn, current_keys, encoded_key})
          else
            sources
          end

        {{node_id, value}, sources}
      end)

    {results, %{state | sources: sources}}
  end

  defp reconcile_index(node_id, old_keys, new_keys, idx) do
    old_set = MapSet.new(old_keys)
    new_set = MapSet.new(new_keys)

    Enum.each(MapSet.difference(old_set, new_set), fn key ->
      :ets.delete_object(NodeDAG.index_table(), {key, node_id})
    end)

    Enum.each(MapSet.difference(new_set, old_set), fn key ->
      :ets.insert(NodeDAG.index_table(), {key, node_id})
    end)

    :ets.insert(NodeDAG.nodes_table(), {node_id, :source, idx, new_keys})
  end

  # Batched per-pid dispatch. For one flush, builds pid → [(node_id, value)]
  # by walking the affected nodes' Group memberships once each, then sends a
  # single `{:dag_values, list}` per pid. Cuts subscriber-side wakeups from
  # O(affected_nodes_per_lv) to 1 per flush.
  defp dispatch_batch(_state, []), do: :ok

  defp dispatch_batch(state, pairs) do
    metadata = %{shard: state.idx, pair_count: length(pairs)}

    :telemetry.span([:upkeep, :node_dag, :dispatch], metadata, fn ->
      pairs_by_pid =
        Enum.reduce(pairs, %{}, fn {node_id, value}, acc ->
          {_load_fn, _keys, encoded_key} = Map.fetch!(state.sources, node_id)

          NodeDAG.group()
          |> Group.members(encoded_key)
          |> Enum.reduce(acc, fn {pid, _meta}, acc ->
            Map.update(acc, pid, [{node_id, value}], &[{node_id, value} | &1])
          end)
        end)

      pid_count = map_size(pairs_by_pid)

      Enum.each(pairs_by_pid, fn {pid, batched} ->
        send(pid, {:dag_values, Enum.reverse(batched)})
      end)

      {:ok, Map.put(metadata, :pid_count, pid_count)}
    end)
  end

  ## Shard-restart hygiene

  defp sweep_owned_ets(idx) do
    NodeDAG.nodes_table()
    |> :ets.match_object({:_, :_, idx, :_})
    |> Enum.each(fn {node_id, _kind, ^idx, keys} ->
      Enum.each(keys, &:ets.delete_object(NodeDAG.index_table(), {&1, node_id}))
      :ets.delete(NodeDAG.nodes_table(), node_id)
    end)
  end

  defp bump_generation(idx) do
    case :ets.info(@generation_table) do
      :undefined ->
        :ets.new(@generation_table, [:set, :public, :named_table, write_concurrency: true])

      _ ->
        :ok
    end

    :ets.update_counter(@generation_table, idx, 1, {idx, 0})
  end
end
