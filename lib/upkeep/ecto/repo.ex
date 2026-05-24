defmodule Upkeep.Ecto.Repo do
  @moduledoc """
  Ecto Repo wrapper that captures row changes for Upkeep.

  Phoenix applications can use this in their Repo instead of `Ecto.Repo`.
  Replace `use Ecto.Repo` with `use Upkeep.Ecto.Repo` and keep the repo's
  existing options, including its adapter:

      use Upkeep.Ecto.Repo, ...

  The generated Repo keeps the normal Ecto API while emitting `Upkeep.Change`
  notifications for successful `insert`, `update`, `insert_or_update`, and
  `delete` calls, as well as `insert_all`, `update_all`, and `delete_all`.

  ## Precondition: writes must go through the wrapped repo

  Upkeep only sees writes that flow through a repo built with `use Upkeep.Ecto.Repo`.
  This is a hard precondition:

  > Every write that should trigger refreshes MUST go through the wrapped repo.

  Out-of-band writes do **not** emit `Upkeep.Change` events, so watched sources
  keep serving stale data. Writes Upkeep cannot see include:

    * a plain `Ecto.Repo` (one not built with `use Upkeep.Ecto.Repo`);
    * a second or otherwise unwrapped repo, even against the same database;
    * raw SQL such as `Ecto.Adapters.SQL.query/4` and migrations;
    * bulk operations (`insert_all`/`update_all`/`delete_all`) on an unwrapped repo.

  For unavoidable out-of-band writes, emit the change yourself
  (`Upkeep.updated/2`, `Upkeep.inserted/2`, `Upkeep.deleted/2`, or
  `Upkeep.changed/3`) or declare an explicit `invalidated_by/2`/`reacts_to/2`
  surface on the affected sources.

  The watch/read-time guard verifies the repo a source *reads* through and can be
  configured to fail fast in production with
  `config :upkeep, repo_capture_misconfiguration: :raise`. It cannot detect
  out-of-band *writes*, which remain the caller's responsibility.

  Captured bulk writes that cannot materialize their affected rows (no schema, an
  uninspectable table source, an adapter without `RETURNING`, a caller-supplied
  `select`, or a table-metadata failure) fall back to a broad, schema/table-wide
  invalidation: every source reading that table refreshes. This is over-broad but
  sound, so a deopted bulk write never silently misses.
  """

  defmacro __using__(opts) do
    quote do
      use Ecto.Repo, unquote(opts)
      use Upkeep.Ecto.RepoCapture
    end
  end

  @doc """
  Returns whether `repo` was built with `use Upkeep.Ecto.Repo`.

  This is primarily used by `Upkeep.Test.assert_repo_capture_enabled!/1` and
  source watch guardrails. Applications should prefer the test helper when
  asserting setup.
  """
  def capture_enabled?(repo) when is_atom(repo) do
    Code.ensure_loaded?(repo) and function_exported?(repo, :__upkeep_repo_capture_enabled__?, 0) and
      repo.__upkeep_repo_capture_enabled__?()
  end

  def capture_enabled?(_repo), do: false
end
