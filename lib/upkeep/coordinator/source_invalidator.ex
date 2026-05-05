defmodule Upkeep.Coordinator.SourceInvalidator do
  @moduledoc false

  use GenServer

  alias Upkeep.Source.ReadCache
  alias Upkeep.Coordinator.Subscriptions

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_) do
    :ok = Subscriptions.join_notifications(:read_node_watcher)
    {:ok, %{}}
  end

  @impl true
  def handle_info({:upkeep_graph_notify, origin, _event}, state) when origin == node() do
    {:noreply, state}
  end

  def handle_info({:upkeep_graph_notify, _origin, event}, state) do
    ReadCache.invalidate(event)
    {:noreply, state}
  end

  def handle_info({:upkeep_graph_notify, event}, state) do
    ReadCache.invalidate(event)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
