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
      Upkeep.Source,
      Upkeep.Source.Coverage
    ]

  alias Ecto.Adapters.SQL.Sandbox
  alias Upkeep.Coordinator.Graph
  alias Upkeep.Ecto.Repo
  alias Upkeep.Ecto.WriteGuard
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
  Assert that every write inside `fun` flowed through `Upkeep.Ecto.Repo`.

  Wraps `fun`, watching the repo's Ecto query telemetry, and fails if any
  write (`INSERT`/`UPDATE`/`DELETE`) reaches the database without going through
  capture — for example raw SQL via `Ecto.Adapters.SQL.query/4`, or an unwrapped
  repo sharing the same telemetry prefix. Such writes silently leave watched
  sources stale, so this turns the repo-capture precondition into an enforceable
  test:

      Upkeep.Test.assert_all_writes_captured(fn ->
        Catalog.import_items(rows)
      end)

  Writes explicitly opted out with `upkeep: false` (or inside
  `Upkeep.with_upkeep(false, ...)`) are intentional and do not fail the
  assertion. Pass `repo:` to target a repo other than the configured one.

  Detection is process-local: writes must run in the calling process (the usual
  case under the SQL sandbox).
  """
  def assert_all_writes_captured(fun, opts \\ []) when is_function(fun, 0) do
    repo = Keyword.get(opts, :repo, default_repo!())
    handler_id = {__MODULE__, :write_guard, System.unique_integer([:positive])}
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        WriteGuard.telemetry_prefix(repo) ++ [:query],
        fn _event, _measurements, metadata, _config ->
          if self() == parent and Repo.observed() == :none and
               WriteGuard.write_sql?(metadata[:query]) do
            send(parent, {handler_id, metadata[:query]})
          end
        end,
        nil
      )

    try do
      fun.()
    after
      :telemetry.detach(handler_id)
    end

    case drain_escaped_writes(handler_id, []) do
      [] -> :ok
      escaped -> raise ExUnit.AssertionError, message: out_of_band_message(repo, escaped)
    end
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

    Configure the repo with `use Upkeep.Ecto.Repo` instead of `use Ecto.Repo`.
    Keep the repo's existing options, including its adapter:

        use Upkeep.Ecto.Repo, ...
    """
  end

  defp allowable_pids do
    [Process.whereis(Graph.task_sup())] |> Enum.reject(&is_nil/1)
  end

  defp drain_escaped_writes(handler_id, acc) do
    receive do
      {^handler_id, query} -> drain_escaped_writes(handler_id, [query | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp out_of_band_message(repo, escaped) do
    queries = escaped |> Enum.uniq() |> Enum.map_join("\n", &"  - #{&1}")

    """
    expected every write through #{inspect(repo)} to flow through Upkeep capture

    #{length(escaped)} write(s) reached the database out-of-band and will not
    refresh watched sources:

    #{queries}

    Route these through a repo built with `use Upkeep.Ecto.Repo`, emit the change
    yourself (`Upkeep.updated/2`, `Upkeep.inserted/2`, `Upkeep.deleted/2`), or, if
    the staleness is intentional, mark the write with `upkeep: false` (or wrap it
    in `Upkeep.with_upkeep(false, ...)`).
    """
  end
end
