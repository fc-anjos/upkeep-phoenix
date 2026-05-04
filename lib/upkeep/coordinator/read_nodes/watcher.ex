defmodule Upkeep.Coordinator.ReadNodes.Watcher do
  @moduledoc """
  Per-node read-node invalidator.

  `Upkeep.Coordinator.Graph.notify/1` dispatches each event through
  `Group.dispatch/3`, which delivers cluster-wide. The mutating node
  invalidates its own read-node cache synchronously inside `notify/1`
  — that preserves the "next read after notify sees fresh data"
  guarantee for the local caller. But remote nodes only learn about
  the event through the dispatched message; without a subscriber,
  their local read-node caches drift stale.

  This Watcher closes that gap: it joins the notification group on
  every node and runs `ReadNodes.invalidate/1` for each event. On the
  originating node the call is a cheap no-op (the cache was already
  cleared by `notify/1`); on remote nodes it does the real work.
  """

  use GenServer

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.ReadNodes

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
  def handle_info({:upkeep_graph_notify, event}, state) do
    ReadNodes.invalidate(event)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
