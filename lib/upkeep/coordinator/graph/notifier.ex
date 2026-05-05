defmodule Upkeep.Coordinator.Graph.Notifier do
  @moduledoc false

  use GenServer

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Topology

  @flush_interval_ms 1
  @flush_threshold 1_000

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def notify(event) when is_struct(event) do
    GenServer.cast(__MODULE__, {:notify, event})
  end

  def drain do
    GenServer.call(__MODULE__, :drain, 60_000)
  end

  @impl true
  def init(_) do
    case Group.join(Graph.group(), Graph.notification_key(), %{kind: :graph_notifier}) do
      :ok -> {:ok, new_state()}
      :already_joined -> {:ok, new_state()}
    end
  end

  @impl true
  def handle_call(:drain, _from, state) do
    {:reply, :ok, flush(state)}
  end

  @impl true
  def handle_cast({:notify, event}, state) do
    {:noreply, enqueue(state, event)}
  end

  @impl true
  def handle_info(:flush, state), do: {:noreply, flush(state)}

  @impl true
  def handle_info({:upkeep_graph_notify, origin, _event}, state) when origin == node() do
    {:noreply, state}
  end

  def handle_info({:upkeep_graph_notify, _origin, event}, state) do
    {:noreply, enqueue(state, event)}
  end

  def handle_info({:upkeep_graph_notify, event}, state) do
    {:noreply, enqueue(state, event)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp new_state do
    %{events: MapSet.new(), flush_scheduled?: false}
  end

  defp enqueue(state, event) do
    events = MapSet.put(state.events, event)
    state = %{state | events: events}

    cond do
      MapSet.size(events) >= @flush_threshold ->
        flush(state)

      state.flush_scheduled? ->
        state

      true ->
        Process.send_after(self(), :flush, @flush_interval_ms)
        %{state | flush_scheduled?: true}
    end
  end

  defp flush(%{events: events} = state) do
    events
    |> Enum.flat_map(&Topology.affected_source_node_ids/1)
    |> Enum.uniq()
    |> Enum.group_by(&Topology.shard_of_node/1)
    |> Enum.each(fn {shard, node_ids} ->
      GenServer.cast(Graph.shard_name(shard), {:notify_source_nodes, node_ids})
    end)

    %{state | events: MapSet.new(), flush_scheduled?: false}
  end
end
