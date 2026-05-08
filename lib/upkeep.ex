defmodule Upkeep do
  @moduledoc """
  Domain-reactive LiveView runtime.

  ## Installation

  Configure the repo Upkeep should use for source reads and mutations:

      config :upkeep, repo: MyApp.Repo

  The repo should use `Upkeep.Ecto.Repo` so committed writes can refresh
  watched sources:

      defmodule MyApp.Repo do
        use Upkeep.Ecto.Repo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.Postgres
      end

  Add Upkeep to your application's supervision tree:

      children = [
        MyApp.Repo,
        {Upkeep, []}
      ]

  See the package guide for complete setup, source authoring, LiveView usage,
  tests, and inspector installation.

  Library users that prefer explicit args can still call
  `Upkeep.mutate(MyApp.Repo, fn -> ... end)` and
  `Upkeep.Test.allow_sandbox(MyApp.Repo)` instead of configuring.

  ## Current Entry Points

  - `mutate/1`, `mutate/2` — transaction boundary that journals notifications.
  - `notify/1` — publish a domain event.
  - `inserted/2`, `updated/2`, `deleted/2`, `changed/3` — typed event helpers.
  - `read/1` — Ecto-backed read inside a source context.
  - `recent_events/1`, `clear_events/0` — observability buffer.

  `updated(record, from: old_record)` has full field-change knowledge.
  `updated(record, changed_fields: fields)` carries write-boundary field
  knowledge. `updated(record)` without either refreshes matching `:updated`
  sources broadly for correctness and emits a diagnostic.

  Optional: add `:upkeep_inspector` to your deps for an in-app dashboard
  that renders the runtime DAG, sources, and telemetry trail.
  """

  use Boundary,
    exports: [
      Observability
    ],
    deps: [
      Group,
      Upkeep.Change,
      Upkeep.Coordinator,
      Upkeep.DAG,
      Upkeep.Ecto,
      Upkeep.Ecto.Source,
      Upkeep.InvalidationSurface,
      Upkeep.Invalidation,
      Upkeep.Mutation,
      Upkeep.Runtime,
      Upkeep.SingleFlight,
      Upkeep.Source,
      {Mix, :compile}
    ]

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
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

  defdelegate mutate(fun), to: Upkeep.Ecto.Mutation
  defdelegate mutate(repo, fun), to: Upkeep.Ecto.Mutation
  defdelegate notify(event), to: Upkeep.Mutation
  defdelegate changed(name, payload, opts \\ []), to: Upkeep.Mutation
  defdelegate inserted(record, opts \\ []), to: Upkeep.Mutation
  defdelegate updated(record, opts \\ []), to: Upkeep.Mutation
  defdelegate deleted(record, opts \\ []), to: Upkeep.Mutation
  defdelegate read(query), to: Upkeep.Ecto.Source
  defdelegate recent_events(opts \\ []), to: Upkeep.Observability, as: :recent
  defdelegate clear_events(), to: Upkeep.Observability, as: :clear
end
