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
       # node_id => {loader, registered_keys, encoded_key, tracked_deps, loaded?}
       sources: %{},
       initial_loads: %{},
       initial_load_refs: %{},
       buffer_node_ids: MapSet.new(),
       buffer_size: 0,
       flush_scheduled?: false
     }}
  end

  @impl true
  def handle_call({:register_source, node_id, interest_keys, loader}, _from, state) do
    state = register_source(state, node_id, interest_keys, loader)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:register_source_and_load, node_id, interest_keys, loader}, from, state) do
    state = register_source(state, node_id, interest_keys, loader)

    case Map.fetch(state.initial_loads, node_id) do
      {:ok, load} ->
        {loader, _registered_keys, _encoded_key, _tracked_deps, _loaded?} =
          Map.fetch!(state.sources, node_id)

        emit_initial_load(:hit, state.idx, node_id, loader)

        state = put_in(state.initial_loads[node_id].waiters, [from | load.waiters])
        {:noreply, state}

      :error ->
        {loader, registered_keys, _encoded_key, _tracked_deps, _loaded?} =
          Map.fetch!(state.sources, node_id)

        emit_initial_load(:miss, state.idx, node_id, loader)

        task =
          Task.Supervisor.async_nolink(Graph.task_sup(), fn ->
            {value, current_keys, tracked_deps} = run_loader_with_deps(loader)
            {node_id, value, current_keys, tracked_deps, loader, registered_keys}
          end)

        state = %{
          state
          | initial_loads:
              Map.put(state.initial_loads, node_id, %{ref: task.ref, waiters: [from]}),
            initial_load_refs: Map.put(state.initial_load_refs, task.ref, node_id)
        }

        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:register_derived, node_id, dep_ids, compute_fn}, _from, state) do
    encoded_key = Graph.source_key(node_id)
    Index.put_derived(node_id, state.idx)

    state = %{
      state
      | dag: DAG.put_derived(state.dag, node_id, dep_ids, compute_fn, initial_value: nil),
        sources: Map.put_new(state.sources, node_id, {nil, [], encoded_key, [], true})
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
  def handle_info(
        {ref, {node_id, value, current_keys, tracked_deps, loader, registered_keys}},
        state
      ) do
    Process.demonitor(ref, [:flush])

    {load, state} = pop_initial_load(state, ref, node_id)

    if current_keys != registered_keys do
      Index.reconcile_source(node_id, state.idx, registered_keys, current_keys)
    end

    {dag, _changed?} = DAG.put_source(state.dag, node_id, value, [])

    sources =
      Map.put(
        state.sources,
        node_id,
        {loader, current_keys, Graph.source_key(node_id), tracked_deps, true}
      )

    Enum.each(load.waiters, &GenServer.reply(&1, {:ok, value, tracked_deps}))

    {:noreply, %{state | dag: dag, sources: sources}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.initial_load_refs, ref) do
      {:ok, node_id} ->
        {load, state} = pop_initial_load(state, ref, node_id)

        :telemetry.execute(
          [:upkeep, :graph, :source_load, :exception],
          %{count: 1},
          %{shard: state.idx, reason: reason}
        )

        Enum.each(load.waiters, &GenServer.reply(&1, {:error, reason}))
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

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

  defp register_source(state, node_id, interest_keys, loader) do
    case Map.fetch(state.sources, node_id) do
      {:ok, {_loader, _registered_keys, _encoded_key, _tracked_deps, _loaded?}} ->
        state

      :error ->
        encoded_key = Graph.source_key(node_id)
        Index.put_source(node_id, state.idx, interest_keys)
        {dag, _changed?} = DAG.put_source(state.dag, node_id, nil, [])

        %{
          state
          | sources:
              Map.put(state.sources, node_id, {loader, interest_keys, encoded_key, [], false}),
            dag: dag
        }
    end
  end

  defp pop_initial_load(state, ref, node_id) do
    load = Map.fetch!(state.initial_loads, node_id)

    state = %{
      state
      | initial_loads: Map.delete(state.initial_loads, node_id),
        initial_load_refs: Map.delete(state.initial_load_refs, ref)
    }

    {load, state}
  end

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
          {loader, registered_keys, encoded_key, _tracked_deps, _loaded?} =
            Map.fetch!(state.sources, node_id)

          {value, current_keys, tracked_deps} = run_loader_with_deps(loader)
          {node_id, value, current_keys, tracked_deps, loader, registered_keys, encoded_key}
        end,
        ordered: false,
        timeout: 30_000,
        on_timeout: :kill_task
      )
      |> Enum.reduce({[], state.sources}, fn
        {:ok, {node_id, value, current_keys, tracked_deps, loader, registered_keys, encoded_key}},
        {results, sources} ->
          sources =
            if current_keys != registered_keys do
              Index.reconcile_source(node_id, state.idx, registered_keys, current_keys)
              Map.put(sources, node_id, {loader, current_keys, encoded_key, tracked_deps, true})
            else
              Map.update!(sources, node_id, fn {loader, keys, encoded_key, _deps, _loaded?} ->
                {loader, keys, encoded_key, tracked_deps, true}
              end)
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

  defp run_loader_with_deps({:source, source, params}) do
    {value, deps} = Upkeep.Source.load(source, params)
    dep_keys = Upkeep.Source.deps_interest_keys(deps)
    {value, Enum.uniq(source.__upkeep_interest_keys__(params) ++ dep_keys), deps}
  end

  defp run_loader_with_deps({:fun, load_fn}) do
    {value, current_keys} = load_fn.()
    {value, current_keys, []}
  end

  defp emit_initial_load(result, shard, node_id, loader) do
    :telemetry.execute(
      [:upkeep, :graph, :initial_load, result],
      %{count: 1},
      Map.merge(%{shard: shard, node_id: node_id}, loader_metadata(loader))
    )
  end

  defp loader_metadata({:source, source, params}), do: %{source: source, params: params}
  defp loader_metadata({:fun, _load_fn}), do: %{source: nil, params: nil}
  defp loader_metadata(nil), do: %{source: nil, params: nil}

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
          {_loader, _keys, encoded_key, _tracked_deps, _loaded?} =
            Map.fetch!(state.sources, node_id)

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
