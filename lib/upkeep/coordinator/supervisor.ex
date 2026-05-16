defmodule Upkeep.Coordinator.Supervisor do
  @moduledoc false

  use Supervisor

  alias Upkeep.Coordinator.SourceProcesses
  alias Upkeep.Coordinator.Topology

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    _opts = opts
    Topology.init_tables()

    children =
      SourceProcesses.child_specs() ++
        [
          Upkeep.Coordinator.Graph.Notifier
        ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
