defmodule Upkeep.Internal.Coordinator.Graph.Shard.InitialLoads do
  @moduledoc false

  alias Upkeep.Internal.Coordinator.Graph
  alias Upkeep.Internal.Coordinator.Graph.Shard.Loaders
  alias Upkeep.Internal.Coordinator.Node
  alias Upkeep.Internal.Coordinator.Topology
  alias Upkeep.Internal.DAG.Store
  alias Upkeep.Internal.SingleFlight

  def register_source_and_load(state, node_id, from) do
    node = Store.fetch_metadata!(state.store, node_id)

    case SingleFlight.join(state.source_loads, node_id, from) do
      {:joined, _load, source_loads} ->
        emit_source(:hit, state.idx, node_id, node.loader)
        {:noreply, %{state | source_loads: source_loads}}

      :no_load ->
        emit_source(:miss, state.idx, node_id, node.loader)
        load_metadata = source_load_metadata(state, node_id, node)

        task =
          Task.Supervisor.async_nolink(Graph.task_sup(), fn ->
            {value, current_keys, tracked_deps} =
              Loaders.run_with_deps(node.loader, load_metadata)

            {node_id, value, current_keys, tracked_deps, node}
          end)

        source_loads = SingleFlight.start(state.source_loads, node_id, task.ref, from, node)
        {:noreply, %{state | source_loads: source_loads}}
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
    case SingleFlight.join(state.derived_loads, node_id, from) do
      {:joined, _load, derived_loads} ->
        emit_derived(:hit, state.idx, node_id, dep_ids, metadata)
        {:noreply, %{state | derived_loads: derived_loads}}

      :no_load ->
        emit_derived(:miss, state.idx, node_id, dep_ids, metadata)

        task =
          Task.Supervisor.async_nolink(Graph.task_sup(), fn ->
            {node_id, compute_fn.(dep_values)}
          end)

        derived_loads = SingleFlight.start(state.derived_loads, node_id, task.ref, from)
        {:noreply, %{state | derived_loads: derived_loads}}
    end
  end

  def handle_source_result(state, ref, node_id, value, current_keys, tracked_deps, %Node{} = node) do
    case SingleFlight.pop(state.source_loads, ref) do
      {:ok, ^node_id, load, source_loads} ->
        Process.demonitor(ref, [:flush])

        if current_keys != node.registered_keys do
          Topology.reconcile_source(node_id, state.idx, node.registered_keys, current_keys)
        end

        {store, _changed?} = Store.put_source(state.store, node_id, value, [])

        store =
          Store.put_metadata(
            store,
            node_id,
            %Node{node | registered_keys: current_keys, tracked_deps: tracked_deps, loaded?: true}
          )

        SingleFlight.reply_all(load, {:ok, value, tracked_deps})

        %{state | store: store, source_loads: source_loads}

      _ ->
        state
    end
  end

  def handle_derived_result(state, ref, _node_id, value) do
    case SingleFlight.pop(state.derived_loads, ref) do
      {:ok, _key, load, derived_loads} ->
        Process.demonitor(ref, [:flush])
        SingleFlight.reply_all(load, {:ok, value})
        %{state | derived_loads: derived_loads}

      :stale ->
        state
    end
  end

  def handle_down(state, ref, reason) do
    case SingleFlight.pop(state.source_loads, ref) do
      {:ok, node_id, load, source_loads} ->
        emit_source_exception(state, node_id, load.extra, reason)
        SingleFlight.reply_all(load, {:error, reason})
        %{state | source_loads: source_loads}

      :stale ->
        case SingleFlight.pop(state.derived_loads, ref) do
          {:ok, _key, load, derived_loads} ->
            :telemetry.execute(
              [:upkeep, :graph, :derived_initial, :exception],
              %{count: 1},
              %{shard: state.idx, reason: reason}
            )

            SingleFlight.reply_all(load, {:error, reason})
            %{state | derived_loads: derived_loads}

          :stale ->
            state
        end
    end
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

  defp emit_source_exception(state, node_id, %Node{} = node, reason) do
    metadata =
      node.loader
      |> Loaders.exception_metadata(reason)
      |> Map.merge(source_load_metadata(state, node_id, node))

    :telemetry.execute([:upkeep, :graph, :source_load, :exception], %{count: 1}, metadata)
  end

  defp source_load_metadata(state, node_id, %Node{} = node) do
    node.loader
    |> Loaders.metadata()
    |> Map.put(:shard, state.idx)
    |> Map.put(:node_id, node_id)
    |> Map.put(:subscriber_count, subscriber_count(node))
  end

  defp subscriber_count(%Node{encoded_key: encoded_key}) do
    Graph.group()
    |> Group.members(encoded_key)
    |> length()
  end
end
