defmodule Upkeep.Coordinator do
  @moduledoc """
  Durable coordination boundary for Upkeep source refresh dispatch.

  The coordinator is a local-first DurableServer. It owns domain fact fanout
  while LiveView processes remain ephemeral Group members.
  """

  use DurableServer, vsn: 1

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor
  @key "coordinators/default"

  def ensure_started(supervisor \\ @supervisor) do
    case DurableServer.Supervisor.ensure_started_child(
           supervisor,
           {__MODULE__, key: @key, initial_state: %{"notified_count" => 0}},
           local_only: true
         ) do
      {:ok, {pid, _meta}} -> {:ok, pid}
      {:error, _reason} = error -> error
    end
  end

  def notify(event) when is_struct(event) do
    with {:ok, pid} <- ensure_started() do
      GenServer.call(pid, {:notify, event})
    end
  end

  @impl true
  def dump_state(state) do
    %{"notified_count" => notified_count(state)}
  end

  @impl true
  def load_state(_old_vsn, dumped_state) do
    %{
      notified_count: notified_count(dumped_state)
    }
  end

  @impl true
  def init(state) do
    {:ok, state, permanent: true, meta: %{type: "upkeep_coordinator"}}
  end

  @impl true
  def handle_call({:notify, event}, _from, state) do
    pids = interested_pids(event)

    :telemetry.span(
      [:upkeep, :coordinator, :dispatch],
      %{event: event},
      fn ->
        Enum.each(pids, &send(&1, {:upkeep_event, event}))

        {:ok, %{event: event, pid_count: length(pids)}}
      end
    )

    {:reply, :ok, %{state | notified_count: state.notified_count + 1}}
  end

  defp interested_pids(event) do
    event
    |> Source.event_keys()
    |> Enum.map(&Source.group_key/1)
    |> Enum.uniq()
    |> Enum.flat_map(&Group.members(@supervisor, &1))
    |> Enum.map(fn {pid, _meta} -> pid end)
    |> Enum.uniq()
  end

  defp notified_count(state) do
    Map.get(state, :notified_count) || Map.get(state, "notified_count", 0)
  end
end
