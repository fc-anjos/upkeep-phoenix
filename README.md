# Upkeep

Upkeep is a Phoenix LiveView runtime for domain-reactive server UI. LiveViews
watch named sources, writes emit domain facts, and Upkeep refreshes the watched
assigns whose source invalidation surface matches those facts.

Start with the package guide in [docs/getting-started.md](docs/getting-started.md),
the public API boundary in [docs/public-api.md](docs/public-api.md), and the
pre-1.0 release policy in [docs/release-policy.md](docs/release-policy.md).
The current roadmap lives in [docs/roadmap.md](docs/roadmap.md).

## Installation

Add Upkeep to your Phoenix application:

```elixir
def deps do
  [
    {:upkeep, "~> 0.1.0"}
  ]
end
```

Use `Upkeep.Ecto.Repo` for the repo whose writes should refresh sources:

```elixir
defmodule MyApp.Repo do
  use Upkeep.Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end
```

Configure the default repo and supervise Upkeep:

```elixir
# config/config.exs
config :upkeep, repo: MyApp.Repo
```

```elixir
# lib/my_app/application.ex
children = [
  MyApp.Repo,
  {Upkeep, []}
]
```

## A Source

Query-backed sources are the preferred Ecto path. Upkeep can inspect the query,
derive invalidation keys, and execute the read through its shared read-node
cache.

```elixir
defmodule MyApp.Issues.OpenIssues do
  use Upkeep.Source, repo: MyApp.Repo

  import Ecto.Query

  def query(%{project_id: project_id}) do
    from issue in MyApp.Issues.Issue,
      where: issue.project_id == ^project_id and issue.status == "open",
      order_by: [asc: issue.position]
  end
end
```

Custom `load/1` sources are supported when they either call `Upkeep.read/1` for
Ecto reads or declare explicit invalidators:

```elixir
defmodule MyApp.Issues.ExternalIssues do
  use Upkeep.Source

  invalidated_by(:external_issues_synced, on: :project_id)

  def load(%{project_id: project_id}) do
    MyApp.ExternalIssues.fetch(project_id)
  end
end
```

## A LiveView

Use `Upkeep.Live` and call `watch/4` from `mount/3` or another LiveView callback.

```elixir
defmodule MyAppWeb.BoardLive do
  use MyAppWeb, :live_view
  use Upkeep.Live

  def mount(%{"project_id" => project_id}, _session, socket) do
    socket =
      watch(socket, :issues, MyApp.Issues.OpenIssues,
        project_id: String.to_integer(project_id)
      )

    {:ok, socket}
  end
end
```

Writes made through a repo using `Upkeep.Ecto.Repo` are captured automatically.
For domain events outside Ecto, notify Upkeep explicitly:

```elixir
Upkeep.mutate(fn ->
  MyApp.ExternalIssues.sync!(project)
  Upkeep.changed(:external_issues_synced, %{project_id: project.id})
end)
```

When calling `Upkeep.updated(record)` manually, pass `from: old_record` for
field-aware invalidation. Without old state, Upkeep refreshes matching
`:updated` sources broadly for correctness and emits a diagnostic.

## Test Setup

Add setup assertions to host app tests:

```elixir
setup do
  Upkeep.Test.assert_repo_capture_enabled!(MyApp.Repo)
  Upkeep.Test.assert_source_reactive!(MyApp.Issues.OpenIssues, %{project_id: 1})
end
```

If your tests use `Ecto.Adapters.SQL.Sandbox`, allow the coordinator processes
to share the test connection after checking out or starting the sandbox owner:

```elixir
Upkeep.Test.allow_sandbox(MyApp.Repo)
```

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
defmodule MyAppWeb.BoardLive do
  use MyAppWeb, :live_view
  use Upkeep.Live
  use Upkeep.Inspector
end
```

Visit a watched LiveView with `?_upkeep=dag` or `?_upkeep=inspect`.

## Demo

The Kanban demo app lives in `examples/kanban` and depends on this package with
`{:upkeep, path: "../.."}`.

```sh
cd examples/kanban
mix setup
mix phx.server
```

Then visit [localhost:4000](http://localhost:4000).
