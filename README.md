# Upkeep

Upkeep keeps Phoenix LiveView assigns in sync with the data they read.

A LiveView watches named sources. When a write commits through your repo,
Upkeep finds the sources that write touches and reloads only the matching
assigns, so a mutation handler can perform the domain action and leave the
refreshing to the data graph the view already declared:

```elixir
def handle_event("rename_item", %{"item" => %{"id" => id, "name" => name}}, socket) do
  {:ok, _item} = Catalog.rename_item(String.to_integer(id), name)
  {:noreply, socket}
end
```

Without Upkeep, that handler would also need to reload the item list, the
counters, and every other assign derived from the changed rows, which couples
each write path to every part of the UI that reads the data. With Upkeep, the
view declares what it reads once and the write paths stay small.

## Core concepts

A **source** is a named, parameterized read: a module that loads one value,
usually from an Ecto query, given stable params such as
`%{project_id: project_id}`. The pair `{SourceModule, params}` is the source's
identity, so two LiveViews watching the same module with the same params share
one loaded value.

A source's **surface** is the set of writes that would make its value stale.
For Ecto sources Upkeep derives the surface from the query itself: a
`where: item.project_id == ^42` filter produces a surface that matches writes
on the `items` table where `project_id` is `42`. Sources that read from
somewhere other than Ecto declare their surface explicitly with
`invalidated_by(...)`. On commit, Upkeep matches the write against every
active surface and refreshes only the sources it touches.

A **derive** is a pure function of other watched values, local to one
LiveView. It recomputes whenever its inputs change and never hits the
database; derives shape source values into the form a particular view needs.

## Quick start

Upkeep is not yet on Hex, so add it from Git and pin a revision:

```elixir
def deps do
  [
    {:upkeep, github: "fc-anjos/upkeep-phoenix", ref: "main"}
  ]
end
```

Replace `use Ecto.Repo` with `use Upkeep.Ecto.Repo` in every repo whose
committed writes should refresh watched sources, keeping the repo's existing
options, and point Upkeep at your default repo:

```elixir
# config/config.exs
config :upkeep, repo: MyApp.Repo
```

`Upkeep.Ecto.Repo` keeps the public repo API and captures committed inserts,
updates, deletes, bulk writes, transactions, and `Ecto.Multi` operations.

Define a source by returning the query it should load:

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

Then watch it from a LiveView:

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

`watch/4` assigns the loaded value to `socket.assigns.items` and keeps it
fresh; `derive/4` computes `:item_count` from it. After a captured write
commits, Upkeep reloads `ProjectItems`, recomputes `:item_count`, and sends
the updated assigns to the LiveView.

## The capture precondition

Upkeep only sees writes that flow through a wrapped repo. A write made
through a plain `Ecto.Repo`, a second unwrapped repo, or raw SQL does not
refresh anything, and the watched sources keep serving stale data until an
unrelated invalidation happens to touch them. If you must write out of band,
emit the change yourself with `Upkeep.updated/2` and friends, and use
`Upkeep.Test.assert_all_writes_captured/1`, `Upkeep.attach_write_guard/2`, or
`mix upkeep.audit` to catch writes that slip past capture. The
[repo capture guide](docs/guides/repo-capture.md) covers this in detail,
including how to intentionally opt a write out with `upkeep: false`.

## Status

Upkeep is early alpha. Application code should stick to the documented entry
points: `Upkeep`, `Upkeep.Ecto.Repo`, `Upkeep.Ecto.Source`, `Upkeep.Source`,
`Upkeep.Live`, and `Upkeep.Test`. Telemetry and internal modules may change.

## Learn more

- [Repo capture](docs/guides/repo-capture.md): opting out, catching
  out-of-band writes, transactions, and SQLite configuration.
- [Sources](docs/guides/sources.md): custom `load/1` reads, non-Ecto
  sources, and emitting domain facts.
- [Identity and authorization](docs/guides/identity-and-authorization.md):
  sources that depend on `:current_scope`.
- [Testing](docs/guides/testing.md): sandbox setup and synchronous
  assertions.

## License

Upkeep is released under the MIT License.
