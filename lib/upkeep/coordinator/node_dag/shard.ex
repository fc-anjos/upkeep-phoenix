defmodule Upkeep.Coordinator.NodeDAG.Shard do
  @moduledoc false
  use GenServer

  alias Upkeep.Coordinator.NodeDAG

  @flush_interval_ms 1
  @flush_threshold 1_000

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    idx = Keyword.fetch!(opts, :idx)
    GenServer.start_link(__MODULE__, idx, name: name)
  end

  @impl true
  def init(idx) do
    {:ok,
     %{
       idx: idx,
       interests: %{},
       pid_nodes: %{},
       monitors: %{},
       buffer_node_ids: MapSet.new(),
       buffer_size: 0,
       flush_scheduled?: false
     }}
  end

  ## Registration

  @impl true
  def handle_call({:register_source, pid, node_id, interest_keys, load_fn}, _from, state) do
    unless :ets.member(NodeDAG.nodes_table(), node_id) do
      :ets.insert(NodeDAG.nodes_table(), {node_id, {:source, load_fn, interest_keys}})

      Enum.each(interest_keys, fn key ->
        :ets.insert(NodeDAG.index_table(), {key, node_id})
      end)
    end

    {:reply, :ok, add_interest(state, pid, node_id)}
  end

  @impl true
  def handle_call({:register_derived, pid, node_id, dep_ids, compute_fn}, _from, state) do
    unless :ets.member(NodeDAG.nodes_table(), node_id) do
      :ets.insert(NodeDAG.nodes_table(), {node_id, {:derived, compute_fn, dep_ids}})

      Enum.each(dep_ids, fn dep ->
        :ets.insert(NodeDAG.dependents_table(), {dep, node_id})
      end)
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
    dirty_sources = MapSet.to_list(state.buffer_node_ids)

    # Step 1: load all dirty source nodes (in parallel via Task.Supervisor).
    # Step 2: compute affected derived nodes (only those whose deps are dirty).
    # Step 3: dispatch values to interested pids — in parallel.

    interests = state.interests
    sources_loaded = load_sources(dirty_sources)

    # Push source values to subscribers (parallel).
    Enum.each(sources_loaded, fn {node_id, value} ->
      pids = Map.get(interests, node_id, MapSet.new())

      if MapSet.size(pids) > 0 do
        msg = {:dag_value, node_id, value}
        spawn_dispatch(pids, msg)
      end
    end)

    # Find derived nodes affected by dirty sources (this shard only).
    derived_to_recompute =
      dirty_sources
      |> Enum.flat_map(&:ets.lookup(NodeDAG.dependents_table(), &1))
      |> Enum.map(fn {_dep, derived_id} -> derived_id end)
      |> Enum.uniq()
      |> Enum.filter(&local_node?(&1, state.idx))

    Enum.each(derived_to_recompute, fn node_id ->
      case :ets.lookup(NodeDAG.nodes_table(), node_id) do
        [{^node_id, {:derived, compute_fn, dep_ids}}] ->
          dep_values = read_dep_values(dep_ids)
          value = compute_fn.(dep_values)
          :ets.insert(NodeDAG.values_table(), {node_id, value})

          pids = Map.get(interests, node_id, MapSet.new())

          if MapSet.size(pids) > 0 do
            spawn_dispatch(pids, {:dag_value, node_id, value})
          end

        _ ->
          :ok
      end
    end)

    %{state | buffer_node_ids: MapSet.new(), buffer_size: 0, flush_scheduled?: false}
  end

  defp load_sources(node_ids) do
    # Run loads in parallel via Task.Supervisor; gather results.
    node_ids
    |> Enum.map(fn node_id ->
      Task.Supervisor.async_nolink(NodeDAG.task_sup(), fn ->
        case :ets.lookup(NodeDAG.nodes_table(), node_id) do
          [{^node_id, {:source, load_fn, _keys}}] ->
            value = load_fn.()
            :ets.insert(NodeDAG.values_table(), {node_id, value})
            {node_id, value}

          _ ->
            {node_id, nil}
        end
      end)
    end)
    |> Enum.map(&Task.await(&1, 30_000))
  end

  defp read_dep_values(dep_ids) do
    Enum.into(dep_ids, %{}, fn dep ->
      case :ets.lookup(NodeDAG.values_table(), dep) do
        [{^dep, value}] -> {dep, value}
        _ -> {dep, nil}
      end
    end)
  end

  defp spawn_dispatch(pids, msg) do
    Task.Supervisor.start_child(NodeDAG.task_sup(), fn ->
      Enum.each(pids, &send(&1, msg))
    end)
  end

  defp local_node?(node_id, idx) do
    shards = :persistent_term.get({NodeDAG, :shards})
    :erlang.phash2(node_id, shards) == idx
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
            remove_node_from_index(node_id)
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

    %{state | interests: interests, pid_nodes: pid_nodes}
  end

  defp remove_node_from_index(node_id) do
    case :ets.lookup(NodeDAG.nodes_table(), node_id) do
      [{^node_id, {:source, _load_fn, keys}}] ->
        Enum.each(keys, fn key ->
          :ets.delete_object(NodeDAG.index_table(), {key, node_id})
        end)

        :ets.delete(NodeDAG.nodes_table(), node_id)
        :ets.delete(NodeDAG.values_table(), node_id)

      [{^node_id, {:derived, _compute_fn, dep_ids}}] ->
        Enum.each(dep_ids, fn dep ->
          :ets.delete_object(NodeDAG.dependents_table(), {dep, node_id})
        end)

        :ets.delete(NodeDAG.nodes_table(), node_id)
        :ets.delete(NodeDAG.values_table(), node_id)

      _ ->
        :ok
    end
  end
end
