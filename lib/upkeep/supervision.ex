defmodule Upkeep.Supervision do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Group,
      Upkeep,
      Upkeep.Coordinator,
      Upkeep.Ecto,
      Upkeep.Invalidation,
      Upkeep.Runtime,
      Upkeep.SingleFlight
    ],
    type: :strict

  use Supervisor

  alias Upkeep.Ecto.Repo

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    Repo.verify_configuration()

    children = [
      Upkeep.Observability,
      {Group, name: Upkeep.Group, log: false},
      {Upkeep.Invalidation, []},
      {Upkeep.SingleFlight.Registry,
       name: Upkeep.Runtime.source_load_coalescer_name(),
       telemetry_prefix: [:upkeep, :source, :initial_load]},
      {Upkeep.Coordinator.Graph, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
