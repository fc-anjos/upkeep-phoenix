defmodule Upkeep.Coordinator.Graph.Shard.InitialLoads do
  @moduledoc false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Index
  alias Upkeep.Coordinator.Graph.Shard.Loaders
  alias Upkeep.Coordinator.Node
  alias Upkeep.DAG.Store

  def register_source_and_load(state, node_id, from) do
    case Map.fetch(state.initial_loads, node_id) do
      {:ok, load} ->
        node = Map.fetch!(state.sources, node_id)

        emit_source(:hit, state.idx, node_id, node.loader)

        state = put_in(state.initial_loads[node_id].waiters, [from | load.waiters])
        {:noreply, state}

      :error ->
        node = Map.fetch!(state.sources, node_id)

        emit_source(:miss, state.idx, node_id, node.loader)

        task =
          Task.Supervisor.async_nolink(Graph.task_sup(), fn ->
            {value, current_keys, tracked_deps} = Loaders.run_with_deps(node.loader)
            {node_id, value, current_keys, tracked_deps, node}
          end)

        state = %{
          state
          | initial_loads:
              Map.put(state.initial_loads, node_id, %{ref: task.ref, waiters: [from], node: node}),
            initial_load_refs: Map.put(state.initial_load_refs, task.ref, node_id)
        }

        {:noreply, state}
    end
  end

  def register_derived_and_compute(
        state,
        node_id,
        dep_ids,
        dep_values,
        compute_fn,
        metadata,
        from
      ) do
    case Map.fetch(state.initial_derived_loads, node_id) do
      {:ok, load} ->
        emit_derived(:hit, state.idx, node_id, dep_ids, metadata)

        state = put_in(state.initial_derived_loads[node_id].waiters, [from | load.waiters])
        {:noreply, state}

      :error ->
        emit_derived(:miss, state.idx, node_id, dep_ids, metadata)

        task =
          Task.Supervisor.async_nolink(Graph.task_sup(), fn ->
            {node_id, compute_fn.(dep_values)}
          end)

        state = %{
          state
          | initial_derived_loads:
              Map.put(state.initial_derived_loads, node_id, %{ref: task.ref, waiters: [from]}),
            initial_derived_load_refs: Map.put(state.initial_derived_load_refs, task.ref, node_id)
        }

        {:noreply, state}
    end
  end

  def handle_source_result(
        state,
        ref,
        node_id,
        value,
        current_keys,
        tracked_deps,
        %Node{} = node
      ) do
    case Map.fetch(state.initial_load_refs, ref) do
      {:ok, ^node_id} ->
        Process.demonitor(ref, [:flush])

        {load, state} = pop_source(state, ref, node_id)

        if current_keys != node.registered_keys do
          Index.reconcile_source(node_id, state.idx, node.registered_keys, current_keys)
        end

        {store, _changed?} = Store.put_source(state.store, node_id, value, [])

        sources =
          Map.put(
            state.sources,
            node_id,
            %Node{node | registered_keys: current_keys, tracked_deps: tracked_deps, loaded?: true}
          )

        Enum.each(load.waiters, &GenServer.reply(&1, {:ok, value, tracked_deps}))

        %{state | store: store, sources: sources}

      _stale_reply ->
        state
    end
  end

  def handle_derived_result(state, ref, node_id, value) do
    case Map.fetch(state.initial_derived_load_refs, ref) do
      {:ok, ^node_id} ->
        Process.demonitor(ref, [:flush])

        {load, state} = pop_derived(state, ref, node_id)

        Enum.each(load.waiters, &GenServer.reply(&1, {:ok, value}))

        state

      _stale_reply ->
        state
    end
  end

  def handle_down(state, ref, reason) do
    case Map.fetch(state.initial_load_refs, ref) do
      {:ok, node_id} ->
        {load, state} = pop_source(state, ref, node_id)
        emit_exception([:upkeep, :graph, :source_load, :exception], state, node_id, load, reason)
        Enum.each(load.waiters, &GenServer.reply(&1, {:error, reason}))
        state

      :error ->
        handle_derived_down(state, ref, reason)
    end
  end

  defp handle_derived_down(state, ref, reason) do
    case Map.fetch(state.initial_derived_load_refs, ref) do
      {:ok, node_id} ->
        {load, state} = pop_derived(state, ref, node_id)
        emit_exception([:upkeep, :graph, :derived_initial, :exception], state.idx, reason)
        Enum.each(load.waiters, &GenServer.reply(&1, {:error, reason}))
        state

      :error ->
        state
    end
  end

  defp pop_source(state, ref, node_id) do
    load = Map.fetch!(state.initial_loads, node_id)

    state = %{
      state
      | initial_loads: Map.delete(state.initial_loads, node_id),
        initial_load_refs: Map.delete(state.initial_load_refs, ref)
    }

    {load, state}
  end

  defp pop_derived(state, ref, node_id) do
    load = Map.fetch!(state.initial_derived_loads, node_id)

    state = %{
      state
      | initial_derived_loads: Map.delete(state.initial_derived_loads, node_id),
        initial_derived_load_refs: Map.delete(state.initial_derived_load_refs, ref)
    }

    {load, state}
  end

  defp emit_source(result, shard, node_id, loader) do
    :telemetry.execute(
      [:upkeep, :graph, :initial_load, result],
      %{count: 1},
      Map.merge(%{shard: shard, node_id: node_id}, Loaders.metadata(loader))
    )
  end

  defp emit_derived(result, shard, node_id, dep_ids, metadata) do
    :telemetry.execute(
      [:upkeep, :graph, :derived_initial, result],
      %{count: 1},
      metadata
      |> Map.put(:shard, shard)
      |> Map.put(:node_id, node_id)
      |> Map.put(:dep_node_ids, dep_ids)
    )
  end

  defp emit_exception(event, state, node_id, %{node: %Node{} = node}, reason) do
    metadata =
      node.loader
      |> Loaders.exception_metadata(reason)
      |> Map.put(:shard, state.idx)
      |> Map.put(:node_id, node_id)
      |> Map.put(:subscriber_count, subscriber_count(node))

    :telemetry.execute(event, %{count: 1}, metadata)
  end

  defp emit_exception(event, shard, reason) do
    :telemetry.execute(event, %{count: 1}, %{shard: shard, reason: reason})
  end

  defp subscriber_count(%Node{encoded_key: encoded_key}) do
    Graph.group()
    |> Group.members(encoded_key)
    |> length()
  end
end
