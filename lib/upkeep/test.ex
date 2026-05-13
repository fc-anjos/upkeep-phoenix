defmodule Upkeep.Test do
  @moduledoc """
  Test helpers for resetting Upkeep runtime state, asserting source
  reactivity, and wiring the coordinator into a host app's SQL sandbox.
  """

  use Boundary,
    top_level?: true,
    deps: [
      Ecto.Adapters.SQL,
      ExUnit,
      Upkeep.Coordinator,
      Upkeep.Ecto,
      Upkeep.Invalidation,
      Upkeep.Source
    ]

  alias Ecto.Adapters.SQL.Sandbox
  alias Upkeep.Coordinator.Graph
  alias Upkeep.Ecto.Repo
  alias Upkeep.Source.Coverage

  @doc """
  Reset coordinator graph runtime state between tests.

  SQL sandbox rollback resets database state, but the Upkeep graph is shared
  process state. Call this before seeding or watching sources in tests that may
  reuse source identities across test cases.
  """
  def reset_graph do
    Graph.reset()
    Upkeep.Invalidation.reset()
  end

  @doc """
  Wait until pending Upkeep graph notifications have been processed.

  Use this in tests that need to assert graph-pushed values after a write or
  explicit notification.
  """
  def await_idle, do: Graph.drain()

  @doc """
  Run `fun` and wait until Upkeep has processed the notifications it emitted.

  This is the preferred test helper when the test performs a mutation and then
  immediately asserts on watched LiveView assigns or graph-pushed values.
  """
  def sync(fun) when is_function(fun, 0) do
    fun.()
  after
    await_idle()
  end

  @doc """
  Return true if `pid` is subscribed to an Upkeep source node.

  This is a test diagnostic for assertions around watcher lifecycle. Host
  applications should not use coordinator internals directly.
  """
  def subscribed?(node_id, pid \\ self()), do: Graph.subscribed?(node_id, pid)

  @doc """
  Assert that Upkeep can see a source's invalidation surface.

  This executes the source once so custom `load/1` and `load/2` callbacks can
  report any `Upkeep.read/1` queries they perform.
  """
  def assert_source_reactive!(source, params) when is_atom(source) and is_map(params) do
    coverage = Upkeep.Source.coverage(source, params)

    if Coverage.known?(coverage) do
      coverage
    else
      raise ExUnit.AssertionError,
        message:
          "expected #{inspect(source)} with params #{inspect(params)} to have a known " <>
            "Upkeep invalidation surface\n" <> Coverage.explain(coverage)
    end
  end

  @doc """
  Assert that a repo was configured with `use Upkeep.Ecto.Repo`.

  Use this in host app tests to catch the highest-risk silent setup mistake:
  sources can load successfully through a plain `Ecto.Repo`, but writes will
  not automatically notify Upkeep unless repo capture is installed.
  """
  def assert_repo_capture_enabled!(repo \\ default_repo!())

  def assert_repo_capture_enabled!(repo) when is_atom(repo) do
    if Repo.capture_enabled?(repo) do
      :ok
    else
      raise ExUnit.AssertionError,
        message: repo_capture_assertion_message(repo)
    end
  end

  def assert_repo_capture_enabled!(repo) do
    raise ExUnit.AssertionError,
      message: repo_capture_assertion_message(repo)
  end

  @doc """
  Allow the calling test pid's sandboxed connection to be used by the
  coordinator's shard processes and their task supervisor.

  Must be called after `Ecto.Adapters.SQL.Sandbox.start_owner!/2` (or
  `checkout/2`) in the same test process. Idempotent — re-running has
  no effect for already-allowed pids.

  Without arguments, uses the repo configured via `config :upkeep, repo: ...`.
  """
  def allow_sandbox(repo \\ default_repo!())

  def allow_sandbox(repo) when is_atom(repo) do
    Enum.each(allowable_pids(), fn pid ->
      Sandbox.allow(repo, self(), pid)
    end)

    :ok
  end

  defp default_repo! do
    Application.get_env(:upkeep, :repo) ||
      raise """
      No default repo configured for Upkeep. Either pass the repo to
      `Upkeep.Test.allow_sandbox/1`, or configure one in your app:

          config :upkeep, repo: MyApp.Repo
      """
  end

  defp repo_capture_assertion_message(repo) do
    """
    expected #{inspect(repo)} to be capture-enabled for Upkeep

    Configure the repo with `use Upkeep.Ecto.Repo` instead of `use Ecto.Repo`:

        defmodule MyApp.Repo do
          use Upkeep.Ecto.Repo,
            otp_app: :my_app,
            adapter: Ecto.Adapters.Postgres
        end
    """
  end

  defp allowable_pids do
    shard_pids =
      for idx <- 0..(Graph.shard_count() - 1), do: Process.whereis(Graph.shard_name(idx))

    [Process.whereis(Graph.task_sup()) | shard_pids] |> Enum.reject(&is_nil/1)
  end
end
