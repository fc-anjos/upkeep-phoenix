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

      # In your test case template (e.g. Upkeep.DataCase):
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

  defp allowable_pids do
    shard_pids =
      for idx <- 0..(Graph.shard_count() - 1), do: Process.whereis(Graph.shard_name(idx))

    [Process.whereis(Graph.task_sup()) | shard_pids] |> Enum.reject(&is_nil/1)
  end
end
