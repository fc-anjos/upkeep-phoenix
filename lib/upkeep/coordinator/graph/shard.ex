defmodule Upkeep.Coordinator.Graph.Shard do
  @moduledoc false
  use GenServer

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Index
  alias Upkeep.DAG

  @flush_interval_ms 1
  @flush_threshold 1_000
  @generation_table :upkeep_graph_shard_generations

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
    :ok = Group.monitor(Graph.group(), "graph/source/")

    generation = bump_generation(idx)

    :ok =
      Group.register(Graph.group(), Graph.shard_key(idx), %{
        pid: self(),
        generation: generation
      })

    {:ok,
     %{
       idx: idx,
       generation: generation,
       dag: DAG.new(),
       # node_id => {loader, registered_keys, encoded_key}
       sources: %{},
       buffer_node_ids: MapSet.new(),
       buffer_size: 0,
       flush_scheduled?: false
     }}
  end

  @impl true
  def handle_call({:register_source, node_id, interest_keys, loader}, _from, state) do
    encoded_key = Graph.source_key(node_id)
    Index.put_source(node_id, state.idx, interest_keys)
    {dag, _changed?} = DAG.put_source(state.dag, node_id, nil, [])

    state = %{
      state
      | sources: Map.put(state.sources, node_id, {loader, interest_keys, encoded_key}),
        dag: dag
    }

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:register_derived, node_id, dep_ids, compute_fn}, _from, state) do
    encoded_key = Graph.source_key(node_id)
    Index.put_derived(node_id, state.idx)

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
        if Group.member_count(Graph.group(), key) == 0,
          do: remove_node(state, node_id),
          else: state
    end
  end

  defp handle_group_event(_other, state), do: state

  defp decode_owned_source_key(key, idx) do
    node_id = Graph.decode_source_key(key)

    case Index.lookup(node_id) do
      {:ok, {_kind, ^idx, _keys}} -> node_id
      _ -> :other
    end
  rescue
    _ -> :other
  end

  defp remove_node(state, node_id) do
    Index.delete(node_id)

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
    {results, sources} =
      Task.Supervisor.async_stream_nolink(
        Graph.task_sup(),
        node_ids,
        fn node_id ->
          {loader, registered_keys, encoded_key} = Map.fetch!(state.sources, node_id)
          {value, current_keys} = run_loader(loader)
          {node_id, value, current_keys, loader, registered_keys, encoded_key}
        end,
        ordered: false,
        timeout: 30_000,
        on_timeout: :kill_task
      )
      |> Enum.reduce({[], state.sources}, fn
        {:ok, {node_id, value, current_keys, loader, registered_keys, encoded_key}},
        {results, sources} ->
          sources =
            if current_keys != registered_keys do
              Index.reconcile_source(node_id, state.idx, registered_keys, current_keys)
              Map.put(sources, node_id, {loader, current_keys, encoded_key})
            else
              sources
            end

          {[{node_id, value} | results], sources}

        {:exit, reason}, {results, sources} ->
          :telemetry.execute(
            [:upkeep, :graph, :source_load, :exception],
            %{count: 1},
            %{shard: state.idx, reason: reason}
          )

          {results, sources}
      end)

    {Enum.reverse(results), %{state | sources: sources}}
  end

  defp run_loader({:source, source, params}) do
    {value, deps} = Upkeep.Source.load(source, params)
    dep_keys = Upkeep.Source.deps_interest_keys(deps)
    {value, Enum.uniq(source.__upkeep_interest_keys__(params) ++ dep_keys)}
  end

  defp run_loader({:fun, load_fn}), do: load_fn.()

  # Batched per-pid dispatch. For one flush, builds pid → [(node_id, value)]
  # by walking the affected nodes' Group memberships once each, then sends a
  # single `{:dag_values, list}` per pid. Cuts subscriber-side wakeups from
  # O(affected_nodes_per_lv) to 1 per flush.
  defp dispatch_batch(_state, []), do: :ok

  defp dispatch_batch(state, pairs) do
    metadata = %{shard: state.idx, pair_count: length(pairs)}

    :telemetry.span([:upkeep, :graph, :dispatch], metadata, fn ->
      pairs_by_pid =
        Enum.reduce(pairs, %{}, fn {node_id, value}, acc ->
          {_loader, _keys, encoded_key} = Map.fetch!(state.sources, node_id)

          Graph.group()
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
    idx
    |> Index.owned_nodes()
    |> Enum.each(fn {node_id, _kind, ^idx, _keys} ->
      Index.delete(node_id)
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
