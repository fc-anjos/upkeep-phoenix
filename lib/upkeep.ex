defmodule Upkeep do
  @moduledoc """
  Domain-reactive LiveView runtime.

  ## Installation

  Configure the repo Upkeep should use for source reads and mutations:

      config :upkeep, repo: MyApp.Repo

  The repo should use `Upkeep.Ecto.Repo` so committed writes can refresh
  watched sources. Replace `use Ecto.Repo` with `use Upkeep.Ecto.Repo`
  and keep the repo's existing options, including its adapter:

      use Upkeep.Ecto.Repo, ...

  Upkeep starts through its own OTP application when included as a normal
  runtime dependency. Do not add `{Upkeep, []}` to your Phoenix application's
  supervision tree unless you have intentionally disabled dependency
  application startup and are managing Upkeep manually.

  See the README for complete setup, source authoring, LiveView usage, and
  tests.

  Library users that prefer explicit args can still call
  `Upkeep.mutate(MyApp.Repo, fn -> ... end)` and
  `Upkeep.Test.allow_sandbox(MyApp.Repo)` instead of configuring.

  ## Current Entry Points

  - `mutate/1`, `mutate/2` — transaction boundary that journals notifications.
  - `notify/1` — publish a domain event.
  - `inserted/2`, `updated/2`, `deleted/2`, `changed/3` — typed event helpers.
  - `read/1` — Ecto-backed read inside a source context.
  - `current_scope!/1` — identity read inside an identity-aware source context.
  - `recent_events/1`, `clear_events/0` — observability buffer.

  `updated(record, from: old_record)` has full field-change knowledge.
  `updated(record, changed_fields: fields)` carries write-boundary field
  knowledge. `updated(record)` without either refreshes matching `:updated`
  sources broadly for correctness and emits a diagnostic.

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

  @doc """
  Run a mutation inside the configured repo transaction and dispatch any
  Upkeep notifications after the transaction commits.
  """
  defdelegate mutate(fun), to: Upkeep.Ecto.Mutation

  @doc """
  Run a mutation inside the given repo transaction and dispatch any Upkeep
  notifications after the transaction commits.
  """
  defdelegate mutate(repo, fun), to: Upkeep.Ecto.Mutation

  @doc """
  Publish a domain event to Upkeep's invalidation runtime.
  """
  defdelegate notify(event), to: Upkeep.Mutation

  @doc """
  Publish a semantic domain change.
  """
  defdelegate changed(name, payload, opts \\ []), to: Upkeep.Mutation

  @doc """
  Publish an inserted-record change.
  """
  defdelegate inserted(record, opts \\ []), to: Upkeep.Mutation

  @doc """
  Publish an updated-record change.

  Pass `from: old_record` when possible so field-indexed invalidation can
  refresh the old and new matching source sets precisely.
  """
  defdelegate updated(record, opts \\ []), to: Upkeep.Mutation

  @doc """
  Publish a deleted-record change.
  """
  defdelegate deleted(record, opts \\ []), to: Upkeep.Mutation

  @doc """
  Execute an Ecto read inside a source context so Upkeep can capture the
  source's invalidation surface.

  This is only valid from a source `load/1`, `load/2`, `query/1`, or `query/2`
  callback. For ad-hoc queries outside a source, call your repo directly.
  """
  defdelegate read(query), to: Upkeep.Ecto.Source

  @doc """
  Return Phoenix's `:current_scope` value inside an identity-aware source
  callback.
  """
  defdelegate current_scope!(context), to: Upkeep.Source

  @doc """
  Return recent diagnostic runtime events.
  """
  defdelegate recent_events(opts \\ []), to: Upkeep.Observability, as: :recent

  @doc """
  Clear the diagnostic runtime event buffer.
  """
  defdelegate clear_events(), to: Upkeep.Observability, as: :clear
end
