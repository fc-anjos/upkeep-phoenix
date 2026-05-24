defmodule Upkeep.Coordinator.RuntimeSupervisor do
  @moduledoc false

  use Supervisor

  alias Upkeep.Coordinator.DerivedProcesses
  alias Upkeep.Coordinator.SourceProcesses

  @child_ids [
    SourceProcesses.registry(),
    SourceProcesses.supervisor(),
    SourceProcesses.task_sup(),
    DerivedProcesses.registry(),
    DerivedProcesses.supervisor(),
    DerivedProcesses.task_sup(),
    Upkeep.Coordinator.Graph.Notifier,
    Upkeep.Coordinator.LifecycleMonitor
  ]

  @default_drain_timeout 5_000
  @drain_poll_ms 5

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def drain(timeout \\ @default_drain_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_drain(deadline)
  end

  @impl true
  def init(_opts) do
    children =
      SourceProcesses.child_specs() ++
        DerivedProcesses.child_specs() ++
        [
          Upkeep.Coordinator.Graph.Notifier,
          Upkeep.Coordinator.LifecycleMonitor
        ]

    # Later workers depend on earlier registries/supervisors.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp do_drain(deadline) do
    cond do
      children_started?() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :ok

      true ->
        Process.sleep(@drain_poll_ms)
        do_drain(deadline)
    end
  end

  defp children_started? do
    children =
      __MODULE__
      |> Supervisor.which_children()
      |> Map.new(fn {id, pid, _type, _modules} -> {id, pid} end)

    Enum.all?(@child_ids, fn id -> is_pid(Map.get(children, id)) end)
  catch
    :exit, _reason -> false
  end
end
