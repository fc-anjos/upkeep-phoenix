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

  Non-Ecto or opaque reads should use `Upkeep.Source` with `load/1` and
  explicit invalidators.
  """

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Ecto.Adapters.SQL,
      Ecto.Query,
      Ecto.SubQuery,
      Logger,
      Upkeep.Change,
      Upkeep.Invalidation,
      Upkeep.Source,
      {Mix, :compile}
    ],
    type: :strict

  defmacro __using__(opts) do
    opts = Keyword.put(opts, :query_adapter, __MODULE__)

    quote do
      use Upkeep.Source.Spec, unquote(opts)
    end
  end

  @doc false
  def read(%Ecto.Query{} = query) do
    case Upkeep.Source.Loader.read_context() do
      %{repo: repo, holder: holder, source: source, params: params} ->
        Upkeep.Ecto.Source.RepoCaptureGuard.ensure_repo_capture!(repo, source, params,
          boundary: :read
        )

        deps = Upkeep.Ecto.Source.QueryDeps.from_query(query)
        Upkeep.Source.Loader.track_dependency(deps)

        fingerprint = read_fingerprint(repo, query)

        Upkeep.Source.Loader.memoized_read(
          fingerprint,
          fn ->
            node_id = {:read, repo, fingerprint}

            Upkeep.Invalidation.fetch_read(
              node_id,
              deps,
              fn -> repo.all(query) end,
              holder
            )
          end
        )

      _ ->
        raise ArgumentError,
              "Upkeep.read/1 must be called inside a source context. " <>
                "Use it only inside a source's load/1 or query/1 callback. " <>
                "For ad-hoc queries, call Repo.all/1 directly."
    end
  end

  def read(value), do: value

  @doc false
  def verify_source!(source, params, opts \\ []) do
    Upkeep.Ecto.Source.RepoCaptureGuard.verify_source!(source, params, opts)
  end

  @doc false
  def query_interest_keys(source, params) when is_atom(source) do
    source
    |> source_query(params)
    |> Upkeep.Ecto.Source.QueryDeps.interest_keys()
  end

  @doc false
  def query_reacts_to?(source, event, params) when is_atom(source) and is_struct(event) do
    deps =
      source
      |> source_query(params)
      |> Upkeep.Ecto.Source.QueryDeps.from_query()

    Upkeep.Ecto.Source.QueryDeps.matches_change?(deps, event)
  end

  defp source_query(source, params) do
    if function_exported?(source, :query, 1), do: source.query(params), else: nil
  end

  defp read_fingerprint(repo, query) do
    {sql, params} = Ecto.Adapters.SQL.to_sql(:all, repo, query)
    :erlang.phash2({sql, params})
  end
end
