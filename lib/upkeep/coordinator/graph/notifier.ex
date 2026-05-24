defmodule Upkeep.Coordinator.Graph.Notifier do
  @moduledoc false

  use GenServer

  alias Upkeep.Coordinator.SourceProcesses
  alias Upkeep.Coordinator.Topology

  @max_batch_messages 1_000

  # Cap on the drain-barrier pending set (see `track_pending/2`). Bounds memory
  # in production, where `drain` (the only consumer) is never called.
  @max_pending_barrier 10_000

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
      |> barrier_pending()

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

  defp new_state(pending \\ MapSet.new()) do
    %{events: MapSet.new(), message_count: 0, pending: pending}
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

  defp flush(%{events: events, message_count: message_count, pending: pending}) do
    started_at = System.monotonic_time()

    source_node_ids =
      events
      |> Enum.flat_map(&Topology.affected_source_node_ids/1)
      |> Enum.uniq()

    # Non-blocking delivery: one slow/wedged source must not stall invalidation
    # delivery to the others, nor block the single Notifier funnel. The cast is
    # sent from this Notifier process, so its ordering relative to a later
    # `barrier_pending/1` call (also from this process) is FIFO-guaranteed.
    Enum.each(source_node_ids, &SourceProcesses.invalidate_async/1)

    duration = System.monotonic_time() - started_at

    emit_flush(events, message_count, source_node_ids, duration)

    new_state(track_pending(pending, source_node_ids))
  end

  # The pending set is only consumed by the drain barrier (test determinism);
  # `drain` is never called in production, so cap the set to avoid unbounded
  # growth. Test batches are tiny, so nothing is dropped under the cap.
  defp track_pending(pending, source_node_ids) do
    if MapSet.size(pending) >= @max_pending_barrier do
      MapSet.new(source_node_ids)
    else
      MapSet.union(pending, MapSet.new(source_node_ids))
    end
  end

  # Drain barrier: block until every source we cast to since the last drain has
  # APPLIED its invalidation. Tests rely on `Graph.drain()`/`Upkeep.Test.sync`
  # for determinism. Because the invalidation casts and this barrier call are
  # both issued from the Notifier process, Erlang's per-pair FIFO ordering
  # guarantees each cast is processed before its barrier call returns. The
  # barrier is a pure no-op ping (`sync/1`) so it never triggers an extra
  # reload.
  defp barrier_pending(%{pending: pending} = state) do
    Enum.each(pending, &SourceProcesses.sync/1)
    %{state | pending: MapSet.new()}
  end

  defp emit_flush(events, message_count, source_node_ids, duration) do
    event_count = MapSet.size(events)

    if event_count > 0 do
      :telemetry.execute(
        [:upkeep, :graph, :notifier, :flush],
        %{count: 1, duration: duration},
        %{
          message_count: message_count,
          event_count: event_count,
          source_node_count: length(source_node_ids),
          source_process_count: length(source_node_ids)
        }
      )
    end
  end
end
