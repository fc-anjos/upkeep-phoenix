defmodule Upkeep.Coordinator.Supervisor do
  @moduledoc false

  use Supervisor

  alias Upkeep.Coordinator.Topology

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    _opts = opts

    # Keep topology tables outside the runtime restart chain.
    table_owner =
      Upkeep.ETS.TableOwner.child_specs(
        name: Upkeep.Coordinator.Topology.TableOwner,
        tables: Topology.table_specs()
      )

    children =
      table_owner ++
        [
          Upkeep.Coordinator.RuntimeSupervisor
        ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
