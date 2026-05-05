defmodule Upkeep.Test do
  @moduledoc """
  Test-mode integration with `Ecto.Adapters.SQL.Sandbox`.

  The coordinator's shards and their task supervisor start with the
  application — long before any test acquires a sandbox connection.
  Without explicit allowance, the shards' load tasks open their own DB
  connection, which races with migrations and other tests' transactions
  (manifests as `database is busy` on SQLite, lock timeouts on Postgres).

  This module wires the long-lived coordinator processes into the
  current test's sandboxed connection so all DB activity flows through
  one transaction that rolls back at test end.

  ## Usage

      # In your test case template (e.g. Upkeep.TestSupport.DataCase):
      setup tags do
        pid = Ecto.Adapters.SQL.Sandbox.start_owner!(MyApp.Repo,
                shared: not tags[:async])

        Upkeep.Test.allow_sandbox(MyApp.Repo)

        on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
        :ok
      end

  Or with the configured default repo (`config :upkeep, repo: MyApp.Repo`):

      Upkeep.Test.allow_sandbox()
  """

  alias Upkeep.Coordinator.Graph

  @doc """
  Reset coordinator graph runtime state between tests.

  SQL sandbox rollback resets database state, but the Upkeep graph is shared
  process state. Call this before seeding or watching sources in tests that may
  reuse source identities across test cases.
  """
  def reset_graph do
    Graph.reset()
  end

  @doc """
  Assert that Upkeep can see a source's invalidation surface.

  This executes the source once so custom `load/1` callbacks can report any
  `Upkeep.read/1` queries they perform.
  """
  def assert_source_reactive!(source, params) when is_atom(source) and is_map(params) do
    coverage = Upkeep.Source.coverage(source, params)

    if Upkeep.Source.Coverage.known?(coverage) do
      coverage
    else
      raise ExUnit.AssertionError,
        message:
          "expected #{inspect(source)} with params #{inspect(params)} to have a known " <>
            "Upkeep invalidation surface\n" <> Upkeep.Source.Coverage.explain(coverage)
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
    if Upkeep.Ecto.Repo.capture_enabled?(repo) do
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
      Ecto.Adapters.SQL.Sandbox.allow(repo, self(), pid)
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
