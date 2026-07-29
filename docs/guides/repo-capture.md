# Repo capture

Ecto-backed sources refresh automatically when the source exposes what it
reads and the repo emits committed writes. This guide covers how capture
behaves at the edges: opting out, writes Upkeep cannot see, and the tools
that catch them.

## Opting out

A single write can opt out with `upkeep: false`. To run existing code such as
context functions, scripts, or console sessions without refreshing any
watcher, wrap it in `Upkeep.with_upkeep/2` instead of threading the option
through every call:

```elixir
# A console fix or backfill that must not refresh connected LiveViews.
Upkeep.with_upkeep(false, fn ->
  Catalog.rename_item(id, name)
  Catalog.rebuild_counts()
end)
```

Every write inside the block uses the given value as its capture default, so
unmodified context code stops emitting `Upkeep.Change` events without needing
a separate non-capturing repo. An explicit `upkeep:` option on an individual
write still wins, and `with_upkeep/2` nests, so
`Upkeep.with_upkeep(true, fn -> ... end)` re-enables capture for a region
inside a disabled block. The previous default is restored when the block
returns or raises.

This is the supported way to make an intentional, non-refreshing change. If
those rows should still appear on screen eventually, emit one broad refresh
afterward with `Upkeep.updated(SomeSchema)` rather than one per row.

Reactivity is opt-in on both sides, so most non-reactive data needs no
special handling. If data is on screen and should stay current, watch a
source for it. A point-in-time snapshot or an on-demand report can be read
plainly and never watched. A write that should never refresh the UI, such as
an audit row or a cache rebuild, gets `upkeep: false` so the intent is
explicit.

## The precondition

Every write that should trigger refreshes must go through a repo built with
`use Upkeep.Ecto.Repo`. Out-of-band writes do not emit `Upkeep.Change`
events, so watched sources keep serving stale data until the next unrelated
invalidation. Writes Upkeep cannot see include:

```elixir
# 1. A plain Ecto.Repo (not built with `use Upkeep.Ecto.Repo`)
MyApp.PlainRepo.update!(changeset)

# 2. A second/unwrapped repo, even against the same database
MyApp.ReportingRepo.insert_all(Invoice, rows)

# 3. Raw SQL, including Ecto.Adapters.SQL.query/4 and migrations
Ecto.Adapters.SQL.query!(MyApp.Repo, "UPDATE items SET archived = true", [])

# 4. Bulk ops on an unwrapped repo
MyApp.PlainRepo.delete_all(Item)
```

If you must write out of band, emit the change yourself so sources still
refresh, for example `Upkeep.updated(record, from: old_record)`, or the broad
`Upkeep.updated(record)` when you lack the old row, or declare an explicit
`invalidated_by(...)` surface and notify it.

Even captured bulk writes can fall back to a broad, schema-wide invalidation
when Upkeep cannot materialize the affected rows (no schema, an uninspectable
table source, an adapter without `RETURNING`, a caller-supplied `select`, or
a table-metadata failure). The refresh is still correct, just less selective.

Inside transactions, Upkeep journals captured changes and dispatches them
only when the outer transaction commits.

Upkeep starts through its own OTP application when it is included as a
normal runtime dependency, so Phoenix applications should not add
`{Upkeep, []}` to their own supervision tree. Applications that disable
dependency application startup can start
`{Upkeep.Supervision, name: Upkeep.Supervisor}` manually.

If an Ecto-backed source uses a plain `Ecto.Repo`, Upkeep catches that at
watch/read time. The default policy raises in dev/test and warns in prod:

```elixir
config :upkeep, repo_capture_misconfiguration: :raise
# or :warn / :ignore
```

Set `:raise` in prod to turn the precondition into a hard, fail-fast check
so a non-capturing source can never silently ship. This guard verifies the
repo a source reads through; it cannot detect out-of-band writes through a
second repo or raw SQL, which remain the caller's responsibility.

At boot, Upkeep also warns once, without blocking startup, when
`config :upkeep, repo: ...` points at a repo that is not built with
`use Upkeep.Ecto.Repo`. Silence it with
`repo_capture_misconfiguration: :ignore` if that repo only backs
explicit-only sources.

## Catching out-of-band writes

Because the failure mode is silent staleness, Upkeep ships tools that turn
the precondition into something you can enforce.

In tests, wrap a path with `Upkeep.Test.assert_all_writes_captured/1`. It
fails if any `INSERT`/`UPDATE`/`DELETE` reaches the database without flowing
through capture, while ignoring writes you deliberately marked
`upkeep: false`:

```elixir
test "import refreshes watched sources" do
  Upkeep.Test.assert_all_writes_captured(fn ->
    Catalog.import_items(rows)
  end)
end
```

At runtime in dev, opt in from your application start to get a warning the
moment a write slips past capture:

```elixir
# lib/my_app/application.ex, after the repo has started
if Application.get_env(:my_app, :env) == :dev do
  Upkeep.attach_write_guard(MyApp.Repo)
end
```

It logs a warning for each out-of-band write, ignoring `upkeep: false`
writes. It never raises, because a raising telemetry handler would detach
itself. Set a default with `config :upkeep, out_of_band_writes: :warn |
:ignore`, or pass `policy: :ignore` to disable, and call
`Upkeep.detach_write_guard(MyApp.Repo)` to stop it. Attachment is explicit
rather than automatic at boot so it binds to your repo only once the repo is
running.

Across the codebase, `mix upkeep.audit` statically lists the common
out-of-band shapes, such as raw SQL or an unwrapped `use Ecto.Repo`, so each
can be confirmed intentional. It parses the AST, so comments and docs are
never flagged, and it is advisory: it always exits successfully.

None of these can see writes that never reach your app (psql, another
service, a database trigger). For those, emit the change yourself or accept
the staleness.

## SQLite

`ecto_sqlite3` apps must set `default_transaction_mode: :immediate` in every
environment. Without it, WAL pool connections can hold stale read snapshots
and miss commits made through sibling connections:

```elixir
config :my_app, MyApp.Repo,
  database: "...",
  pool_size: 5,
  journal_mode: :wal,
  busy_timeout: 30_000,
  default_transaction_mode: :immediate
```
