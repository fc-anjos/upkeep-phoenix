defmodule Upkeep.Ecto.Repo do
  @moduledoc """
  Ecto Repo wrapper that captures row changes for Upkeep.

  Phoenix applications can use this in their Repo instead of `Ecto.Repo`.
  Replace `use Ecto.Repo` with `use Upkeep.Ecto.Repo` and keep the repo's
  existing options, including its adapter:

      use Upkeep.Ecto.Repo, ...

  The generated Repo keeps the normal Ecto API while emitting `Upkeep.Change`
  notifications for successful `insert`, `update`, `insert_or_update`, and
  `delete` calls.
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
