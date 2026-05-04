# Upkeep

Upkeep is a Phoenix/LiveView runtime for domain-reactive server UI: rendered
views watch named sources, mutations emit domain facts, and sources own their
invalidation semantics.

See the full project roadmap in [docs/roadmap.md](docs/roadmap.md).

## Installation

Add Upkeep to your Phoenix application:

```elixir
def deps do
  [
    {:upkeep, "~> 0.1.0"}
  ]
end
```

Configure the repo Upkeep should use for source reads and mutations:

```elixir
config :upkeep, repo: MyApp.Repo
```

Then define sources with your application repo:

```elixir
defmodule MyApp.Issues.OpenIssues do
  use Upkeep.Source, repo: MyApp.Repo

  import Ecto.Query

  def query(%{project_id: project_id}) do
    from issue in MyApp.Issues.Issue,
      where: issue.project_id == ^project_id and issue.status == "open"
  end
end
```

Use `Upkeep.Live` in LiveViews that watch sources:

```elixir
defmodule MyAppWeb.BoardLive do
  use MyAppWeb, :live_view
  use Upkeep.Live

  def mount(_params, _session, socket) do
    {:ok, watch(socket, :issues, MyApp.Issues.OpenIssues, project_id: 1)}
  end
end
```

## Demo

The Kanban demo app lives in `examples/kanban` and depends on this package with
`{:upkeep, path: "../.."}`.

```sh
cd examples/kanban
mix setup
mix phx.server
```

Then visit [`localhost:4000`](http://localhost:4000).
