# Sources

The quick start covers the standard Ecto path, where every LiveView watching
the same source module and params shares the same loaded value. This guide
covers the other shapes a source can take.

Upkeep narrows a source's surface from supported Ecto equality and
membership filters. Query filters it cannot narrow still refresh correctly,
but at a broader schema or table surface.

## Custom Ecto reads

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

`Upkeep.read(query)` is only for Ecto reads inside source callbacks. For
queries outside a source, call your repo directly.

## Non-Ecto sources

Non-Ecto reads, such as ETS, files, caches, external APIs, and process
state, need an explicit surface. Declare it with `invalidated_by(...)`:

```elixir
defmodule MyApp.Search.Sources.Results do
  use Upkeep.Source

  invalidated_by(:search_index_rebuilt, on: :project_id)

  def load(%{project_id: project_id}) do
    MyApp.Search.fetch(project_id)
  end
end
```

## Mutations outside Ecto

Ecto writes are captured automatically when they go through a repo using
`Upkeep.Ecto.Repo`. For work outside Ecto, wrap the mutation and emit a
domain fact:

```elixir
Upkeep.mutate(fn ->
  MyApp.Search.reindex!(project)
  Upkeep.changed(:search_index_rebuilt, %{project_id: project.id})
end)
```

Manual record changes can use `Upkeep.updated(new_item, from: old_item)`:

```elixir
old_item = item
new_item = %{item | project_id: new_project.id}

Upkeep.updated(new_item, from: old_item)
```

When `from:` is omitted, Upkeep cannot prove which field-indexed sources the
record may have moved out of, so it refreshes every matching `:updated`
source and emits `[:upkeep, :change, :broad_update]`.
