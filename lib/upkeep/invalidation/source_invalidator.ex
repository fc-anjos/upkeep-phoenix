defmodule Upkeep.Invalidation.SourceInvalidator do
  @moduledoc false

  use GenServer

  alias Upkeep.Invalidation.{Bus, ReadCache}

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_) do
    :ok = Bus.join(:read_node_watcher)
    {:ok, %{}}
  end

  @impl true
  def handle_info({:upkeep_invalidation, origin, _event}, state) when origin == node() do
    {:noreply, state}
  end

  def handle_info({:upkeep_invalidation, _origin, event}, state) do
    ReadCache.invalidate(event)
    {:noreply, state}
  end

  def handle_info({:upkeep_invalidation, event}, state) do
    ReadCache.invalidate(event)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
