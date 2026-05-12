# Getting Started

This guide shows the smallest useful Upkeep path in a Phoenix LiveView app:
configure the repo, define a source, watch it from a LiveView, and let domain
writes refresh the watched assign.

## Add Upkeep

Dogfood applications in this workspace depend on the package by path. Use the
relative path from the Phoenix app to `phoenix/upkeep`:

```elixir
def deps do
  [
    {:upkeep, path: "../../upkeep"}
  ]
end
```

Use `Upkeep.Ecto.Repo` for every repo whose committed writes should refresh
watched sources:

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

Upkeep starts through its own OTP application when it is included as a normal
runtime dependency, so Phoenix applications should not add `{Upkeep, []}` to
their own supervision tree.

`Upkeep.Ecto.Repo` keeps the normal Ecto API and captures committed inserts,
updates, deletes, bulk writes, direct transactions, and `Ecto.Multi`
operations. A specific write can opt out with `upkeep: false`.

## Define A Source

Use `Upkeep.Ecto.Source` when a source value comes from an Ecto query. The
preferred shape is `query/1`; Upkeep reads the query, derives invalidation
keys, and reloads the source when matching writes commit.

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

The source params are part of the source identity. Two LiveViews watching the
same source module with the same params can share one source load.

## Watch From A LiveView

Use `Upkeep.Live` in the LiveView and call `watch/4` with an assign name,
source module, and params.

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

`watch/4` assigns the loaded value to `socket.assigns.items`. `derive/4`
computes another assign from watched values or earlier derived values.

## Let Writes Refresh The UI

Writes through a repo that uses `Upkeep.Ecto.Repo` notify Upkeep after the
transaction commits. Mutation handlers do the domain action and leave watched
assign refreshes to Upkeep:

```elixir
def handle_event("rename_item", %{"item" => %{"id" => id, "name" => name}}, socket) do
  {:ok, _item} = MyApp.Catalog.rename_item(String.to_integer(id), name)
  {:noreply, socket}
end
```

For work outside Ecto, wrap the mutation and emit a domain fact:

```elixir
Upkeep.mutate(fn ->
  MyApp.Search.reindex!(project)
  Upkeep.changed(:search_index_rebuilt, %{project_id: project.id})
end)
```

Then define a non-Ecto source with an explicit invalidator:

```elixir
defmodule MyApp.Search.Sources.Results do
  use Upkeep.Source

  invalidated_by(:search_index_rebuilt, on: :project_id)

  def load(%{project_id: project_id}) do
    MyApp.Search.fetch(project_id)
  end
end
```

## Choose A Source Shape

Use the narrowest shape that makes every input to the source value visible:

| Need | Shape |
| --- | --- |
| Ecto query, same value for every subscriber with the same params | `use Upkeep.Ecto.Source` and `query/1` |
| Ecto query whose result depends on Phoenix `:current_scope` | `use Upkeep.Ecto.Source` and `query/2` |
| Custom Ecto reads, same value for every subscriber with the same params | `use Upkeep.Ecto.Source`, `load/1`, and `Upkeep.read/1` |
| Custom Ecto reads that depend on Phoenix `:current_scope` | `use Upkeep.Ecto.Source`, `load/2`, `Upkeep.current_scope!/1`, and `Upkeep.read/1` |
| Non-Ecto reads, cache reads, external APIs, ETS, files, or process state | `use Upkeep.Source`, `load/1` or `load/2`, and explicit invalidators |
| Shared source value with viewer-specific presentation | shared `watch/4`, then local `derive/4` from `%{current_scope: scope}` |

`query/2` and `load/2` receive an Upkeep source context as their second
argument. Read Phoenix's `:current_scope` through that context when identity,
tenant, permissions, or policy checks affect the source value:

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

The callback arity marks the source as identity-aware before Upkeep coalesces
or shares source loads. Subscribers with the same params but different
`current_scope` values do not share the source node.

When the loaded value is safe to share and only the presentation differs by
viewer, keep the source shared and make the viewer-specific work local:

```elixir
socket
|> watch(:items, MyApp.Catalog.Sources.ProjectItems, %{project_id: project_id})
|> derive(:visible_items, [:items], fn %{items: items, current_scope: scope} ->
  MyApp.Policy.visible_items(items, scope)
end)
```

Do not hide viewer identity in process state, session state, closures, or
socket captures inside shared source callbacks. Put stable identity in source
params, or read `current_scope` through `Upkeep.current_scope!/1`.

## Test Setup

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
graph-pushed values, wrap the mutation with `Upkeep.Test.sync/1`:

```elixir
Upkeep.Test.sync(fn ->
  Upkeep.changed(:search_index_rebuilt, %{project_id: project.id})
end)
```

## Authorization Checklist

When authorization changes what rows a source may load:

- If the policy can be expressed with stable ids known at the watch site, put
  those ids in source params.
- If the policy reads Phoenix `:current_scope`, use `query/2` or `load/2` and
  call `Upkeep.current_scope!/1`.
- If the source value is shared and authorization only affects display, derive
  a local value from `%{current_scope: scope}`.
- If authorization data changes independently from the source tables, emit a
  domain change and add an invalidator for that change.
