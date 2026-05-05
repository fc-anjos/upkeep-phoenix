defmodule Upkeep do
  @moduledoc """
  Domain-reactive LiveView runtime.

  ## Installation

  Add to your application's supervision tree:

      children = [
        # ...your repo, pubsub, etc...
        {Upkeep, []}
      ]

  Configure your repo so `Upkeep.mutate/1` and `Upkeep.Test.allow_sandbox/0`
  can find it without an explicit argument:

      config :upkeep, repo: MyApp.Repo

  Library users that prefer explicit args can still call
  `Upkeep.mutate(MyApp.Repo, fn -> ... end)` and
  `Upkeep.Test.allow_sandbox(MyApp.Repo)` instead of configuring.

  ## Public API

  - `mutate/1`, `mutate/2` — transaction boundary that journals notifications.
  - `notify/1` — publish a domain event.
  - `inserted/2`, `updated/2`, `deleted/2`, `changed/3` — typed event helpers.
  - `read/1` — Ecto-backed read inside a source context.
  - `recent_events/1`, `clear_events/0` — observability buffer.
  - `introspection_snapshot/2` — symbolic DAG/telemetry document for a LiveView socket.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    children = [
      Upkeep.Observability,
      {Group, name: Upkeep.Group, log: false},
      {Upkeep.Coordinator.Graph, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defdelegate mutate(fun), to: Upkeep.Mutation
  defdelegate mutate(repo, fun), to: Upkeep.Mutation
  defdelegate notify(event), to: Upkeep.Mutation
  defdelegate changed(name, payload, opts \\ []), to: Upkeep.Mutation
  defdelegate inserted(record, opts \\ []), to: Upkeep.Mutation
  defdelegate updated(record, opts \\ []), to: Upkeep.Mutation
  defdelegate deleted(record, opts \\ []), to: Upkeep.Mutation
  defdelegate read(query), to: Upkeep.Source
  defdelegate recent_events(opts \\ []), to: Upkeep.Observability, as: :recent
  defdelegate clear_events(), to: Upkeep.Observability, as: :clear
  defdelegate introspection_snapshot(socket, opts \\ []), to: Upkeep.Introspection, as: :snapshot
end
