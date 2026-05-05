defmodule Upkeep.Internal.Coordinator.ReadNodes.Watcher do
  @moduledoc false

  use GenServer

  alias Upkeep.Internal.Coordinator.Graph
  alias Upkeep.Internal.Coordinator.ReadNodes

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_) do
    case Group.join(Graph.group(), Graph.notification_key(), %{kind: :read_node_watcher}) do
      :ok -> {:ok, %{}}
      :already_joined -> {:ok, %{}}
    end
  end

  @impl true
  def handle_info({:upkeep_graph_notify, origin, _event}, state) when origin == node() do
    {:noreply, state}
  end

  def handle_info({:upkeep_graph_notify, _origin, event}, state) do
    ReadNodes.invalidate(event)
    {:noreply, state}
  end

  def handle_info({:upkeep_graph_notify, event}, state) do
    ReadNodes.invalidate(event)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
