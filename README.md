# Upkeep

Upkeep is a Phoenix LiveView runtime for domain-reactive server UI.

LiveViews watch named sources, writes emit domain facts, and Upkeep refreshes
only the watched assigns whose source invalidation surface matches those facts.

## Why Upkeep

In ordinary LiveView code, a write path often has to know every assign, panel,
counter, sidebar, or derived value that might now be stale:

```elixir
def handle_event("rename_item", %{"item" => params}, socket) do
  {:ok, _item} = Catalog.rename_item(params["id"], params["name"])

  items = Catalog.list_items(socket.assigns.project_id)
  activity = Catalog.recent_activity(socket.assigns.project_id)
  visible_items =
    Catalog.visible_items(socket.assigns.project_id, socket.assigns.current_user.id)

  socket =
    socket
    |> assign(:items, items)
    |> assign(:activity, activity)
    |> assign(:visible_items, visible_items)
    |> assign(:item_count, length(items))
    |> assign(:visible_item_count, length(visible_items))

  {:noreply, socket}
end
```

That works, but the mutation handler is coupled to every UI surface that depends
on the changed data. Adding another dependent view means revisiting old write
paths or broadcasts.

With Upkeep, the LiveView declares the data graph once. The write path performs
the domain action and stops:

```elixir
def handle_event("rename_item", %{"item" => %{"id" => id, "name" => name}}, socket) do
  {:ok, _item} = Catalog.rename_item(String.to_integer(id), name)
  {:noreply, socket}
end
```

Committed Ecto writes are captured through your repo, matching sources reload,
derived values recompute, and unrelated watches are left alone.

## Status

Upkeep is early alpha. Application code should start from the documented public
entry points:

- `Upkeep`
- `Upkeep.Ecto.Repo`
- `Upkeep.Ecto.Source`
- `Upkeep.Source`
- `Upkeep.Live`
- `Upkeep.Test`

Telemetry, internal modules, and undocumented runtime details may change during
alpha.

## Core Concepts

| Concept | What it means |
| --- | --- |
| Repo capture | Your repo uses `Upkeep.Ecto.Repo`, so Upkeep can see committed Ecto writes. |
| Source | A module that knows how to load one value, usually from an Ecto query. |
| Source params | The stable inputs for a source, such as `%{project_id: project_id}`. They are part of the source identity. |
| Watch | A LiveView calls `watch(:assign_name, SourceModule, params)` to load a source into an assign and keep it fresh. |
| Derive | A LiveView calls `derive(:assign_name, deps, fun)` to compute one assign from watched or derived assigns. |
| Mutation | A write that changes domain data. Ecto writes are captured automatically; non-Ecto writes emit explicit facts with `Upkeep.changed(name, metadata)`. |
| Invalidation | The match between a committed write or domain fact and the sources that should reload. |
| Current scope | Phoenix's `:current_scope` assign, treated as viewer identity when a source or derive depends on it. |

## Quick Start

### 1. Add Upkeep

Add Upkeep to your Phoenix app:

```elixir
def deps do
  [
    {:upkeep, "~> 0.1.0"}
  ]
end
```

### 2. Enable Repo Capture

Use `Upkeep.Ecto.Repo` for every repo whose committed writes should refresh
watched sources. Replace `use Ecto.Repo` with `use Upkeep.Ecto.Repo` and keep
the repo's existing options, including whichever Ecto adapter your app already
uses:

```diff
- use Ecto.Repo, ...
+ use Upkeep.Ecto.Repo, ...
```

Configure the default repo:

```elixir
# config/config.exs
config :upkeep, repo: MyApp.Repo
```

`Upkeep.Ecto.Repo` keeps the normal Ecto API and captures committed inserts,
updates, deletes, bulk writes, direct transactions, and `Ecto.Multi` operations.
A specific write can opt out with `upkeep: false`.

Upkeep starts through its own OTP application when it is included as a normal
runtime dependency, so Phoenix applications should not add `{Upkeep, []}` to
their own supervision tree.

Ecto-backed sources refresh automatically when the source exposes what it reads
and the repo emits committed writes. If an Ecto-backed source uses a plain
`Ecto.Repo`, Upkeep catches that at watch/read time. The default policy raises
in dev/test and warns in prod:

```elixir
config :upkeep, repo_capture_misconfiguration: :raise
# or :warn / :ignore
```

### 3. Define A Source

Use `Upkeep.Ecto.Source` when a source value comes from an Ecto query. Define
`query(params)` and return the Ecto query Upkeep should load.

```elixir
defmodule MyApp.Catalog.Sources.ProjectItems do
  use Upkeep.Ecto.Source, repo: MyApp.Repo

  import Ecto.Query

  def query(%{project_id: project_id}) do
    from item in MyApp.Catalog.Item,
      where: item.project_id == ^project_id and item.archived == false,
      order_by: [asc: item.position]
  end
end
```

### 4. Watch The Source

Use `Upkeep.Live` in the LiveView and call `watch(:assign_name, SourceModule,
params)` with an assign name, source module, and params. Pass source params as a
map. Keyword lists are also accepted and normalized to maps, but maps are easier
to read.

```elixir
defmodule MyAppWeb.ProjectLive do
  use MyAppWeb, :live_view
  use Upkeep.Live

  alias MyApp.Catalog.Sources.ProjectItems

  def mount(%{"id" => id}, _session, socket) do
    project_id = String.to_integer(id)

    socket =
      socket
      |> watch(:items, ProjectItems, %{project_id: project_id})
      |> derive(:item_count, [:items], fn %{items: items} -> length(items) end)

    {:ok, socket}
  end
end
```

`watch(:items, ProjectItems, params)` assigns the loaded value to
`socket.assigns.items`. `derive(:item_count, [:items], fun)` computes another
assign from watched values or earlier derived values.

### 5. Mutate Normally

Writes through a repo that uses `Upkeep.Ecto.Repo` notify Upkeep after the
transaction commits. Mutation handlers do the domain action and leave watched
assign refreshes to Upkeep:

```elixir
def handle_event("rename_item", %{"item" => %{"id" => id, "name" => name}}, socket) do
  {:ok, _item} = MyApp.Catalog.rename_item(String.to_integer(id), name)
  {:noreply, socket}
end
```

After the commit, Upkeep reloads `ProjectItems`, recomputes `:item_count`, and
pushes the new assigns to the LiveView.

## Source Shapes

The Quick Start source is the common case: an Ecto query with the same result
for every LiveView watching the same params.

Upkeep derives invalidation keys from supported Ecto equality and membership
filters. Query filters it cannot narrow still refresh correctly, but at a
broader schema or table level.

Custom Ecto reads can use `load(params)` and `Upkeep.read(query)`:

```elixir
defmodule MyApp.Catalog.Sources.ProjectSummary do
  use Upkeep.Ecto.Source, repo: MyApp.Repo

  import Ecto.Query

  def load(%{project_id: project_id}) do
    items =
      from(item in MyApp.Catalog.Item,
        where: item.project_id == ^project_id and item.archived == false
      )
      |> Upkeep.read()

    %{item_count: length(items)}
  end
end
```

`Upkeep.read(query)` is only for Ecto reads inside source callbacks. For queries
outside a source, call your repo directly.

Non-Ecto reads, spawned task reads, ETS, files, caches, external APIs, and
process state need explicit invalidators:

```elixir
defmodule MyApp.Search.Sources.Results do
  use Upkeep.Source

  invalidated_by(:search_index_rebuilt, on: :project_id)

  def load(%{project_id: project_id}) do
    MyApp.Search.fetch(project_id)
  end
end
```

Use `reacts_to(name, fun)` or `reacts_to(name, opts, fun)` when field matching
needs custom logic.

## Mutations

Ecto writes are captured automatically when they go through a repo using
`Upkeep.Ecto.Repo`. For work outside Ecto, wrap the mutation and emit a domain
fact:

```elixir
Upkeep.mutate(fn ->
  MyApp.Search.reindex!(project)
  Upkeep.changed(:search_index_rebuilt, %{project_id: project.id})
end)
```

Manual record changes can use typed helpers:

```elixir
old_item = item
new_item = %{item | project_id: new_project.id}

Upkeep.updated(new_item, from: old_item)
```

When `from:` is omitted, Upkeep cannot prove which field-indexed sources the
record may have moved out of. It refreshes all matching `:updated` sources
broadly for correctness and emits `[:upkeep, :change, :broad_update]`.

## Identity And Authorization

Sources whose rows depend on Phoenix's `:current_scope` use `load(params,
upkeep)` or `query(params, upkeep)`. Read the scope from the `upkeep` context so
different viewers do not share the same source load:

```elixir
defmodule MyApp.Catalog.Sources.VisibleProjectItems do
  use Upkeep.Ecto.Source, repo: MyApp.Repo

  import Ecto.Query

  def query(%{project_id: project_id}, upkeep) do
    scope = Upkeep.current_scope!(upkeep)

    from item in MyApp.Catalog.Item,
      where:
        item.project_id == ^project_id and
          item.account_id == ^scope.account_id and
          item.value <= ^scope.max_item_value,
      order_by: [asc: item.position]
  end
end
```

If every viewer may receive the same loaded rows and only the presentation
changes, keep the source shared and derive the viewer-specific value locally:

```elixir
socket
|> watch(:items, MyApp.Catalog.Sources.ProjectItems, %{project_id: project_id})
|> derive(:visible_items, [:items], fn %{items: items, current_scope: scope} ->
  MyApp.Policy.visible_items(items, scope)
end)
```

Do not hide viewer identity in process state, session state, closures, or socket
captures inside shared source callbacks. Put stable identity in source params,
or read `current_scope` through `Upkeep.current_scope!(upkeep)`.

Upkeep does not inspect your app-specific scope struct to decide what fields
mean account, project, user, or session.

When authorization changes what rows a source may load:

- If the policy can be expressed with stable ids known at the watch site, put
  those ids in source params.
- If the policy reads Phoenix `:current_scope`, use `query(params, upkeep)` or
  `load(params, upkeep)` and call `Upkeep.current_scope!(upkeep)`.
- If the source value is shared and authorization only affects display, derive
  a local value from `%{current_scope: scope}`.
- If authorization data changes independently from the source tables, emit a
  domain change and add an invalidator for that change.

## Testing

Assert that the repo is capture-enabled:

```elixir
test "repo is capture-enabled" do
  assert :ok = Upkeep.Test.assert_repo_capture_enabled!(MyApp.Repo)
end
```

Assert that a source has a known invalidation surface:

```elixir
test "project items source is reactive" do
  Upkeep.Test.assert_source_reactive!(
    MyApp.Catalog.Sources.ProjectItems,
    %{project_id: 1}
  )
end
```

If a test uses `Ecto.Adapters.SQL.Sandbox`, allow Upkeep coordinator processes
to use the checked-out connection:

```elixir
setup do
  :ok = Ecto.Adapters.SQL.Sandbox.checkout(MyApp.Repo)
  Upkeep.Test.allow_sandbox(MyApp.Repo)
  :ok
end
```

When a synchronous test performs a mutation and immediately asserts on
graph-pushed values, wrap the mutation with `Upkeep.Test.sync(fn -> ... end)`:

```elixir
Upkeep.Test.sync(fn ->
  Upkeep.changed(:search_index_rebuilt, %{project_id: project.id})
end)
```

## Adapter Notes

### SQLite

`ecto_sqlite3` apps must set `default_transaction_mode: :immediate` in every
environment. Without it, WAL pool connections can hold stale read snapshots and
miss commits made through sibling connections:

```elixir
config :my_app, MyApp.Repo,
  database: "...",
  pool_size: 5,
  journal_mode: :wal,
  busy_timeout: 30_000,
  default_transaction_mode: :immediate
```

## License

Upkeep is released under the MIT License.
