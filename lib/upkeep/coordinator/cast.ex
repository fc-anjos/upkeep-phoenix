defmodule Upkeep.Coordinator.Cast do
  @moduledoc """
  Plain GenServer notify path using `cast`. No durability, no singleton
  failover. Exists for A/B comparison against the durable coordinator and
  the stateless caller-side fan-out.
  """

  use GenServer

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  def notify(event, server \\ __MODULE__) when is_struct(event) do
    GenServer.cast(server, {:notify, event})
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_cast({:notify, event}, state) do
    event
    |> Source.event_keys()
    |> Enum.map(&Source.group_key/1)
    |> Enum.uniq()
    |> Enum.each(&Group.dispatch(@supervisor, &1, {:upkeep_event, event}))

    {:noreply, state}
  end
end
