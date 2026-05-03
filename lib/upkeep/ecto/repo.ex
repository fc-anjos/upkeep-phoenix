defmodule Upkeep.Ecto.Repo do
  @moduledoc """
  Ecto Repo wrapper that captures row changes for Upkeep.

  Phoenix applications can use this in their Repo instead of `Ecto.Repo`:

      defmodule MyApp.Repo do
        use Upkeep.Ecto.Repo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.Postgres
      end

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
end
