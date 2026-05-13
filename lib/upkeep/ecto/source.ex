defmodule Upkeep.Ecto.Source do
  @moduledoc """
  Ecto-backed source authoring for Upkeep.

  Use this for sources whose value comes from an Ecto query. Upkeep analyzes the
  query shape, tracks the read through `Upkeep.read/1`, and invalidates cached
  reads when matching writes are notified.

      defmodule MyApp.Sources.OpenIssues do
        use Upkeep.Ecto.Source, repo: MyApp.Repo

        import Ecto.Query

        def query(%{project_id: project_id}) do
          from issue in MyApp.Issue,
            where: issue.project_id == ^project_id and issue.closed == false
        end
      end

  Non-Ecto or opaque reads should use `Upkeep.Source` with `load/1` or `load/2` and
  explicit invalidators.
  """

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Upkeep.Ecto.Source.QueryAdapter,
      Upkeep.Source,
      {Mix, :compile}
    ],
    type: :strict

  defmacro __using__(opts) do
    opts = Keyword.put(opts, :query_adapter, Upkeep.Ecto.Source.QueryAdapter)

    quote do
      use Upkeep.Source, unquote(opts)
    end
  end
end
