defmodule Upkeep.Ecto.Source.Reader do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias Upkeep.Ecto.Source.{QueryDeps, RepoCaptureGuard}
  alias Upkeep.Source.Loader

  @spec read(term()) :: term()
  def read(%Ecto.Query{} = query) do
    case Loader.read_context() do
      %{repo: repo, holder: holder, source: source, params: params} ->
        RepoCaptureGuard.ensure_repo_capture!(repo, source, params, boundary: :read)

        deps = QueryDeps.from_query(query)
        :ok = Loader.track_dependency(deps)

        fingerprint = read_fingerprint(repo, query)

        Loader.memoized_read(
          fingerprint,
          fn -> fetch_read(repo, query, fingerprint, deps, holder) end
        )

      _ ->
        raise ArgumentError,
              "Upkeep.read/1 must be called inside a source context. " <>
                "Use it only inside a source's load/1, load/2, query/1, or query/2 callback. " <>
                "For ad-hoc queries, call Repo.all/1 directly."
    end
  end

  def read(value), do: value

  defp read_fingerprint(repo, query) do
    {sql, params} = SQL.to_sql(:all, repo, query)
    :erlang.phash2({sql, params})
  end

  defp fetch_read(repo, query, fingerprint, deps, holder) do
    node_id = {:read, repo, fingerprint}

    Upkeep.Invalidation.fetch_read(
      node_id,
      deps,
      fn -> repo.all(query) end,
      holder
    )
  end
end
