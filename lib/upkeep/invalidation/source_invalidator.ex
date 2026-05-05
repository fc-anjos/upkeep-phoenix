defmodule Upkeep.Invalidation.SourceInvalidator do
  @moduledoc false

  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_) do
    :ok = join_notifications()
    {:ok, %{}}
  end

  @impl true
  def handle_info({:upkeep_graph_notify, origin, _event}, state) when origin == node() do
    {:noreply, state}
  end

  def handle_info({:upkeep_graph_notify, _origin, event}, state) do
    Upkeep.Source.ReadCache.invalidate(event)
    {:noreply, state}
  end

  def handle_info({:upkeep_graph_notify, event}, state) do
    Upkeep.Source.ReadCache.invalidate(event)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp join_notifications do
    group = Upkeep.Coordinator.Graph.group()
    key = Upkeep.Coordinator.Graph.notification_key()

    case Group.join(group, key, %{kind: :read_node_watcher}) do
      :ok -> :ok
      :already_joined -> :ok
    end
  end
end
