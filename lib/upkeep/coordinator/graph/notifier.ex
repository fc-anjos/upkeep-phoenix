defmodule Upkeep.Coordinator.Graph.Notifier do
  @moduledoc false

  use GenServer

  alias Upkeep.Coordinator.Shards
  alias Upkeep.Coordinator.Topology

  @max_batch_messages 1_000

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def drain do
    GenServer.call(__MODULE__, :drain, 60_000)
  end

  @impl true
  def init(_) do
    :ok = Upkeep.Invalidation.join_notifications(:graph_notifier)
    {:ok, new_state()}
  end

  @impl true
  def handle_call(:drain, _from, state) do
    state =
      state
      |> drain_mailbox(:all)
      |> elem(0)
      |> flush()

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:upkeep_invalidation, _origin, event}, state) do
    {:noreply, state |> enqueue(event) |> drain_and_flush()}
  end

  def handle_info({:upkeep_invalidation, event}, state) do
    {:noreply, state |> enqueue(event) |> drain_and_flush()}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp new_state do
    %{events: MapSet.new(), message_count: 0}
  end

  defp enqueue(state, event) do
    %{state | events: MapSet.put(state.events, event), message_count: state.message_count + 1}
  end

  defp drain_and_flush(state) do
    {state, _drained} =
      state
      |> drain_mailbox(@max_batch_messages)

    flush(state)
  end

  defp drain_mailbox(state, limit), do: drain_mailbox(state, limit, 0)

  defp drain_mailbox(state, limit, count) when limit != :all and count >= limit do
    {state, count}
  end

  defp drain_mailbox(state, limit, count) do
    receive do
      {:upkeep_invalidation, _origin, event} ->
        state
        |> enqueue(event)
        |> drain_mailbox(limit, count + 1)

      {:upkeep_invalidation, event} ->
        state
        |> enqueue(event)
        |> drain_mailbox(limit, count + 1)
    after
      0 ->
        {state, count}
    end
  end

  defp flush(%{events: events, message_count: message_count}) do
    started_at = System.monotonic_time()

    routes =
      events
      |> Enum.flat_map(&Topology.affected_source_node_ids/1)
      |> Enum.uniq()
      |> Enum.group_by(&Topology.shard_of_node/1)

    Enum.each(routes, fn {shard, node_ids} ->
      Shards.notify_source_nodes(shard, node_ids)
    end)

    duration = System.monotonic_time() - started_at

    emit_flush(events, message_count, routes, duration)

    new_state()
  end

  defp emit_flush(events, message_count, routes, duration) do
    event_count = MapSet.size(events)

    if event_count > 0 do
      :telemetry.execute(
        [:upkeep, :graph, :notifier, :flush],
        %{count: 1, duration: duration},
        %{
          message_count: message_count,
          event_count: event_count,
          source_node_count:
            routes
            |> Map.values()
            |> Enum.reduce(0, &(length(&1) + &2)),
          shard_count: map_size(routes)
        }
      )
    end
  end
end
