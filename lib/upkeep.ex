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
  application startup and are managing `Upkeep.Supervision` manually.

  See the README for complete setup, source authoring, LiveView usage, and
  tests.

  Library users that prefer explicit args can still call
  `Upkeep.mutate(MyApp.Repo, fn -> ... end)` and
  `Upkeep.Test.allow_sandbox(MyApp.Repo)` instead of configuring.

  ## Current Entry Points

  - `mutate/1`, `mutate/2` — transaction boundary that journals notifications.
  - `with_upkeep/2` — run a block with repo capture forced on or off.
  - `attach_write_guard/2`, `detach_write_guard/1` — warn on out-of-band writes.
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
      Upkeep.Change,
      Upkeep.Ecto,
      Upkeep.Ecto.Source,
      Upkeep.Ecto.Source.Reader,
      Upkeep.Mutation,
      Upkeep.Source,
      {Mix, :compile}
    ]

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
  Run `fun` with repo capture forced on or off for the current process.

  A process-scoped form of the per-write `upkeep: false` option. Writes made by
  any code inside the block — including existing context functions called
  unchanged — skip Upkeep notifications when disabled, without passing
  `upkeep: false` to each call. An explicit `upkeep:` option on an individual
  write still takes precedence.

      Upkeep.with_upkeep(false, fn ->
        Catalog.rename_item(id, name)
      end)
  """
  defdelegate with_upkeep(enabled?, fun), to: Upkeep.Mutation

  @doc """
  Start warning about out-of-band writes through `repo` for the running system.

  Call this once from your application start (after the repo is started), in the
  environments where you want it:

      Upkeep.attach_write_guard(MyApp.Repo)

  It attaches to the repo's Ecto query telemetry and logs a warning whenever a
  write reaches the database without flowing through `Upkeep.Ecto.Repo` capture
  (for example raw SQL), so it would silently leave watched sources stale. Writes
  marked `upkeep: false` are intentional and never warn.

  Pass `policy: :ignore` to make the call a no-op, or set a default with
  `config :upkeep, out_of_band_writes: :warn | :ignore`. The guard only ever
  warns; for hard, fail-the-build enforcement use
  `Upkeep.Test.assert_all_writes_captured/1` in tests.
  """
  defdelegate attach_write_guard(repo, opts \\ []), to: Upkeep.Ecto.WriteGuard, as: :attach

  @doc """
  Stop the out-of-band write guard previously attached to `repo`.
  """
  defdelegate detach_write_guard(repo), to: Upkeep.Ecto.WriteGuard, as: :detach

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
  defdelegate read(query), to: Upkeep.Ecto.Source.Reader

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
