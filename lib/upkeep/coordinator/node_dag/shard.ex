defmodule Upkeep.Coordinator.NodeDAG.Shard do
  @moduledoc false
  use GenServer

  alias Upkeep.Coordinator.NodeDAG
  alias Upkeep.DAG

  @flush_interval_ms 1
  @flush_threshold 1_000

  ## Public

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    idx = Keyword.fetch!(opts, :idx)
    GenServer.start_link(__MODULE__, idx, name: name)
  end

  ## GenServer

  @impl true
  def init(idx) do
    {:ok,
     %{
       idx: idx,
       dag: DAG.new(),
       # node_id => {load_fn, registered_keys}
       sources: %{},
       interests: %{},
       pid_nodes: %{},
       monitors: %{},
       buffer_node_ids: MapSet.new(),
       buffer_size: 0,
       flush_scheduled?: false
     }}
  end

  @impl true
  def handle_call({:register_source, pid, node_id, interest_keys, load_fn}, _from, state) do
    state =
      if Map.has_key?(state.sources, node_id) do
        state
      else
        :ets.insert(NodeDAG.nodes_table(), {node_id, :source, state.idx})
        Enum.each(interest_keys, &:ets.insert(NodeDAG.index_table(), {&1, node_id}))
        {dag, _changed?} = DAG.put_source(state.dag, node_id, nil, [])

        %{
          state
          | sources: Map.put(state.sources, node_id, {load_fn, interest_keys}),
            dag: dag
        }
      end

    {:reply, :ok, add_interest(state, pid, node_id)}
  end

  @impl true
  def handle_call({:register_derived, pid, node_id, dep_ids, compute_fn}, _from, state) do
    state =
      if DAG.has_node?(state.dag, node_id) do
        state
      else
        :ets.insert(NodeDAG.nodes_table(), {node_id, :derived, state.idx})

        %{
          state
          | dag: DAG.put_derived(state.dag, node_id, dep_ids, compute_fn, initial_value: nil)
        }
      end

    {:reply, :ok, add_interest(state, pid, node_id)}
  end

  @impl true
  def handle_call({:unregister, pid, node_id}, _from, state) do
    {:reply, :ok, do_unregister(state, pid, node_id)}
  end

  @impl true
  def handle_call({:subscribers, node_id}, _from, state) do
    {:reply, Map.get(state.interests, node_id, MapSet.new()), state}
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
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    monitors = Map.delete(state.monitors, ref)
    nodes = Map.get(state.pid_nodes, pid, MapSet.new())
    state = Enum.reduce(nodes, %{state | monitors: monitors}, &do_unregister(&2, pid, &1))
    {:noreply, state}
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
    # Loads dirty sources, updates the DAG, recomputes downstream derived
    # via Upkeep.DAG.recompute/2, and dispatches in parallel.
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

    # Dispatch source values unconditionally (matches current contract).
    Enum.each(sources_loaded, fn {id, value} ->
      dispatch(state, id, value, :source)
    end)

    # Dispatch derived only when their value changed.
    Enum.each(derived_changed, fn id ->
      dispatch(state, id, DAG.fetch!(dag, id), :derived)
    end)

    %{
      state
      | dag: dag,
        buffer_node_ids: MapSet.new(),
        buffer_size: 0,
        flush_scheduled?: false
    }
  end

  defp load_sources(node_ids, state) do
    # Run loads in parallel via Task.Supervisor; reconcile drifted keys.
    {results, sources} =
      node_ids
      |> Enum.map(fn node_id ->
        {load_fn, registered_keys} = Map.fetch!(state.sources, node_id)

        task =
          Task.Supervisor.async_nolink(NodeDAG.task_sup(), fn ->
            {value, current_keys} = load_fn.()
            {node_id, value, current_keys}
          end)

        {Task.await(task, 30_000), load_fn, registered_keys}
      end)
      |> Enum.map_reduce(state.sources, fn {{node_id, value, current_keys}, load_fn,
                                            registered_keys},
                                           sources ->
        sources =
          if current_keys != registered_keys do
            reconcile_index(node_id, registered_keys, current_keys)
            Map.put(sources, node_id, {load_fn, current_keys})
          else
            sources
          end

        {{node_id, value}, sources}
      end)

    {results, %{state | sources: sources}}
  end

  defp reconcile_index(node_id, old_keys, new_keys) do
    old_set = MapSet.new(old_keys)
    new_set = MapSet.new(new_keys)

    Enum.each(MapSet.difference(old_set, new_set), fn key ->
      :ets.delete_object(NodeDAG.index_table(), {key, node_id})
    end)

    Enum.each(MapSet.difference(new_set, old_set), fn key ->
      :ets.insert(NodeDAG.index_table(), {key, node_id})
    end)
  end

  defp dispatch(state, node_id, value, node_kind) do
    pids = Map.get(state.interests, node_id, MapSet.new())
    pid_count = MapSet.size(pids)

    if pid_count > 0 do
      msg = {:dag_value, node_id, value}
      metadata = %{shard: state.idx, node_id: node_id, node_kind: node_kind, pid_count: pid_count}

      Task.Supervisor.start_child(NodeDAG.task_sup(), fn ->
        :telemetry.span([:upkeep, :node_dag, :dispatch], metadata, fn ->
          Enum.each(pids, &send(&1, msg))
          {:ok, metadata}
        end)
      end)
    end
  end

  ## Interest bookkeeping

  defp add_interest(state, pid, node_id) do
    interests =
      Map.update(state.interests, node_id, MapSet.new([pid]), &MapSet.put(&1, pid))

    pid_nodes =
      Map.update(state.pid_nodes, pid, MapSet.new([node_id]), &MapSet.put(&1, node_id))

    monitors =
      if Map.has_key?(state.pid_nodes, pid) do
        state.monitors
      else
        ref = Process.monitor(pid)
        Map.put(state.monitors, ref, pid)
      end

    %{state | interests: interests, pid_nodes: pid_nodes, monitors: monitors}
  end

  defp do_unregister(state, pid, node_id) do
    interests =
      case Map.get(state.interests, node_id) do
        nil ->
          state.interests

        set ->
          new_set = MapSet.delete(set, pid)

          if MapSet.size(new_set) == 0 do
            remove_node(state, node_id)
            Map.delete(state.interests, node_id)
          else
            Map.put(state.interests, node_id, new_set)
          end
      end

    pid_nodes =
      case Map.get(state.pid_nodes, pid) do
        nil ->
          state.pid_nodes

        set ->
          new_set = MapSet.delete(set, node_id)

          if MapSet.size(new_set) == 0,
            do: Map.delete(state.pid_nodes, pid),
            else: Map.put(state.pid_nodes, pid, new_set)
      end

    state = %{state | interests: interests, pid_nodes: pid_nodes}

    if not Map.has_key?(state.interests, node_id) do
      %{
        state
        | sources: Map.delete(state.sources, node_id),
          dag: DAG.remove_subgraph(state.dag, node_id)
      }
    else
      state
    end
  end

  defp remove_node(state, node_id) do
    case Map.get(state.sources, node_id) do
      {_load_fn, keys} ->
        Enum.each(keys, fn key ->
          :ets.delete_object(NodeDAG.index_table(), {key, node_id})
        end)

      _ ->
        :ok
    end

    :ets.delete(NodeDAG.nodes_table(), node_id)
  end
end
