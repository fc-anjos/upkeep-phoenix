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

Upkeep is early alpha. Application code should start from the documented entry
points:

- `Upkeep`
- `Upkeep.Ecto.Repo`
- `Upkeep.Ecto.Source`
- `Upkeep.Source`
- `Upkeep.Live`
- `Upkeep.Test`

Modules hidden with `@moduledoc false`, repo-capture implementation modules,
runtime modules, generated-source helpers, query inference modules, coordinator
modules, and test-support modules are internal even though Elixir can import
them. Telemetry is diagnostic during alpha; event names and metadata should be
treated defensively.

## Start Here

The first-use path is in [Getting Started](docs/guides/getting-started.md). It
covers repo setup, source authoring, LiveView usage, mutation refreshes,
identity-aware sources, and test setup.

## Installation

Dogfood Phoenix applications in this workspace depend on Upkeep by path:

```elixir
def deps do
  [
    {:upkeep, path: "../../upkeep"}
  ]
end
```

Use `Upkeep.Ecto.Repo` for each repo whose writes should refresh sources:

```elixir
defmodule MyApp.Repo do
  use Upkeep.Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end
```

Configure the default repo:

```elixir
# config/config.exs
config :upkeep, repo: MyApp.Repo
```

`Upkeep.Ecto.Repo` wraps `Ecto.Repo` and captures committed inserts, updates,
deletes, bulk writes, direct transactions, and `Ecto.Multi` operations. Sources
can still opt out of individual writes with `upkeep: false`.

If a source expects automatic Ecto reactivity but uses a repo that does not use
`Upkeep.Ecto.Repo`, Upkeep checks the repo at watch/read boundaries. The default
policy raises in dev/test and warns in prod:

```elixir
config :upkeep, repo_capture_misconfiguration: :raise
# or :warn / :ignore
```

## A Source

Query-backed sources are the preferred Ecto path. Upkeep can inspect the query,
derive invalidation keys, and execute the read through its shared source
cache.

```elixir
defmodule MyApp.Catalog.ProjectItems do
  use Upkeep.Ecto.Source, repo: MyApp.Repo

  import Ecto.Query

  def query(%{project_id: project_id}) do
    from item in MyApp.Catalog.Item,
      where: item.project_id == ^project_id and item.archived == false,
      order_by: [asc: item.position]
  end
end
```

Upkeep derives invalidation keys from supported Ecto equality and membership
filters. Unsupported but visible query shapes fall back to broad schema/table
invalidation for correctness.

Custom `load/1` and `load/2` sources are supported when they either call
`Upkeep.read/1` for Ecto reads or declare explicit invalidators:

```elixir
defmodule MyApp.Catalog.ProjectSummary do
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

Sources whose value depends on Phoenix's `:current_scope` use `load/2` or
`query/2`. The second argument is an Upkeep source context; reading
`current_scope` through that context makes the source identity include an
opaque scope envelope before source loads are coalesced or shared:

```elixir
defmodule MyApp.Catalog.VisibleProjectItems do
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

Non-Ecto reads, spawned task reads, ETS, files, caches, external APIs, and
process state need explicit invalidators. Hidden process or session state must
not carry viewer identity for a shared source; use `load/2` or `query/2` and
`Upkeep.current_scope!/1` when the source value is identity-sensitive:

```elixir
defmodule MyApp.Search.Results do
  use Upkeep.Source

  invalidated_by(:search_index_rebuilt, on: :project_id)

  def load(%{project_id: project_id}) do
    MyApp.Search.fetch(project_id)
  end
end
```

Use `reacts_to/2` or `reacts_to/3` when field matching needs custom logic.

## A LiveView

Use `Upkeep.Live` and call `watch/4` from `mount/3` or another LiveView
callback. `use Upkeep.Live` imports `watch/4`, `derive/4`, and `component/4`,
and installs the runtime `handle_info/2` needed for graph-pushed values.

```elixir
defmodule MyAppWeb.ProjectLive do
  use MyAppWeb, :live_view
  use Upkeep.Live

  def mount(%{"id" => id}, _session, socket) do
    project_id = String.to_integer(id)

    socket =
      socket
      |> watch(:items, MyApp.Catalog.ProjectItems, project_id: project_id)
      |> derive(:item_count, [:items], fn %{items: items} -> length(items) end)

    {:ok, socket}
  end
end
```

Source params are the default source identity. Keep params explicit and stable.
If user identity, tenant identity, or permissions affect the source value, use
an identity-aware `load/2` or `query/2` source. If the shared source value is
safe for every subscriber and only the presentation is viewer-specific, derive a
local value from `current_scope`.

```elixir
socket
|> watch(:items, MyApp.Catalog.ProjectItems, project_id: project_id)
|> derive(:visible_items, [:items], fn %{items: items, current_scope: scope} ->
  MyApp.Policy.visible_items(items, scope)
end)
```

Upkeep treats Phoenix's `:current_scope` assign as an opaque dependency. It does
not inspect your app-specific scope struct to decide what fields mean account,
project, user, or session.

## Mutations

Writes made through a repo using `Upkeep.Ecto.Repo` are captured automatically.
For domain events outside Ecto, notify Upkeep explicitly:

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
broadly for correctness and emits `[:upkeep, :change, :broad_update]`. Configure
the diagnostic policy with:

```elixir
config :upkeep, update_without_old_state: :warn
# or :ignore
```

## Test Setup

Use setup assertions to catch plain `Ecto.Repo` setup and invisible source
reads:

```elixir
test "repo is capture-enabled" do
  assert :ok = Upkeep.Test.assert_repo_capture_enabled!(MyApp.Repo)
end

test "source is reactive" do
  Upkeep.Test.assert_source_reactive!(
    MyApp.Catalog.ProjectItems,
    %{project_id: 1}
  )
end
```

If your tests use `Ecto.Adapters.SQL.Sandbox`, allow the coordinator processes
to share the test connection after checking out or starting the sandbox owner:

```elixir
Upkeep.Test.allow_sandbox(MyApp.Repo)
```

When a synchronous test performs a mutation and immediately asserts on
graph-pushed values, wrap the mutation with `Upkeep.Test.sync/1`.

## Inspector

The optional inspector package renders a symbolic DAG, source coverage,
invalidation keys, sharing decisions, runtime signals, and recent telemetry.

```elixir
def deps do
  [
    {:upkeep_inspector, "~> 0.1.0", only: [:dev, :test]}
  ]
end
```

```elixir
defmodule MyAppWeb.ProjectLive do
  use MyAppWeb, :live_view
  use Upkeep.Live
  use Upkeep.Inspector
end
```

Visit a watched LiveView with `?_upkeep=dag` or `?_upkeep=inspect`.

Source-location capture is enabled by default in dev/test. In prod it is off
unless explicitly enabled:

```elixir
config :upkeep, capture_source_locations: true
```

The inspector shows symbolic assign shapes, source watches, invalidation keys,
coverage diagnostics, sharing decisions, runtime signals, and recent telemetry.
It does not expose full assign values.

## Examples

Example applications live in the `upkeep-phoenix-examples` repository. In a
local workspace they can depend on this package with a path dependency:

```elixir
{:upkeep, path: "../../upkeep"}
```

## License

Upkeep is released under the MIT License.
