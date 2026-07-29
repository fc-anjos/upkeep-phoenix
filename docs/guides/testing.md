# Testing

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

If a test uses `Ecto.Adapters.SQL.Sandbox`, allow Upkeep coordinator
processes to use the checked-out connection:

```elixir
setup do
  :ok = Ecto.Adapters.SQL.Sandbox.checkout(MyApp.Repo)
  Upkeep.Test.allow_sandbox(MyApp.Repo)
  :ok
end
```

When a test performs a mutation and immediately asserts on refreshed
assigns, wrap the mutation with `Upkeep.Test.sync(fn -> ... end)` so the
refresh has happened by the time the assertion runs:

```elixir
Upkeep.Test.sync(fn ->
  Upkeep.changed(:search_index_rebuilt, %{project_id: project.id})
end)
```
