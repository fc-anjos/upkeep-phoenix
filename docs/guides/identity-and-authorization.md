# Identity and authorization

Sources whose rows depend on Phoenix's `:current_scope` use
`load(params, upkeep)` or `query(params, upkeep)`. Read the scope from the
`upkeep` context so different viewers do not share the same source load:

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

If every viewer may receive the same loaded rows, keep the source shared and
derive viewer-specific assigns locally:

```elixir
socket
|> watch(:items, MyApp.Catalog.Sources.ProjectItems, %{project_id: project_id})
|> derive(:visible_items, [:items], fn %{items: items, current_scope: scope} ->
  MyApp.Policy.visible_items(items, scope)
end)
```

Do not hide viewer identity in process state, session state, closures, or
socket captures inside shared source callbacks. Put stable identity in
source params, or read `current_scope` through `Upkeep.current_scope!(upkeep)`.
Upkeep does not inspect your app-specific scope struct to decide what fields
mean account, project, user, or session.

When authorization changes what rows a source may load:

- If the policy can be expressed with stable ids known at the watch site,
  put those ids in source params.
- If the policy reads Phoenix `:current_scope`, use `query(params, upkeep)`
  or `load(params, upkeep)` and call `Upkeep.current_scope!(upkeep)`.
- If the source value is shared and authorization only affects display,
  derive a local value from `%{current_scope: scope}`.
- If authorization data changes independently from the source tables, emit a
  domain change and add an invalidator for that change.
