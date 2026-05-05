defmodule Upkeep.Source.Loader do
  @moduledoc false

  defdelegate verify_source!(source, params, opts \\ []), to: Upkeep.Source.RepoCaptureGuard

  @context_key {__MODULE__, :read_context}
  @warn_dedup_key {__MODULE__, :no_invalidation_warned}

  def load(source, params) when is_atom(source) do
    repo = source.__upkeep_repo__() || Application.get_env(:upkeep, :repo)

    {value, deps} =
      with_read_context(
        repo,
        Upkeep.Source.Identity.source_id(source, params),
        source,
        params,
        fn ->
          value = source.load(params)
          {value, tracked_deps()}
        end
      )

    coverage = coverage(source, params, deps)

    emit_coverage(coverage)
    warn_if_no_invalidation_surface(coverage)

    {value, deps}
  end

  def read(%Ecto.Query{} = query) do
    case Process.get(@context_key) do
      %{repo: repo} = ctx ->
        Upkeep.Source.RepoCaptureGuard.ensure_repo_capture!(repo, ctx[:source], ctx[:params],
          boundary: :read
        )

        track_query(query)
        memoize_read(repo, ctx[:holder], query)

      _ ->
        raise ArgumentError,
              "Upkeep.read/1 must be called inside a source context. " <>
                "Use it only inside a source's load/1 or query/1 callback. " <>
                "For ad-hoc queries, call Repo.all/1 directly."
    end
  end

  def read(value), do: value

  def coverage(source, params) when is_atom(source) and is_map(params) do
    repo = source.__upkeep_repo__() || Application.get_env(:upkeep, :repo)

    {_value, deps} =
      with_read_context(
        repo,
        Upkeep.Source.Identity.source_id(source, params),
        source,
        params,
        fn ->
          value = source.load(params)
          {value, tracked_deps()}
        end
      )

    coverage(source, params, deps)
  end

  def coverage(source, params, deps) when is_atom(source) and is_map(params) and is_list(deps) do
    deps
    |> Enum.map(&Upkeep.Source.QueryDeps.coverage/1)
    |> Enum.reduce(base_coverage(source, params), &Upkeep.Source.Coverage.merge/2)
    |> attach_unknown_if_empty()
  end

  defp memoize_read(repo, holder, query) do
    fingerprint = read_fingerprint(repo, query)
    cache = Map.get(Process.get(@context_key), :reads, %{})

    case Map.fetch(cache, fingerprint) do
      {:ok, value} ->
        value

      :error ->
        value = Upkeep.Source.ReadCache.fetch_or_load(repo, query, holder)
        ctx = Process.get(@context_key)
        Process.put(@context_key, Map.put(ctx, :reads, Map.put(cache, fingerprint, value)))
        value
    end
  end

  defp read_fingerprint(repo, query) do
    {sql, params} = Ecto.Adapters.SQL.to_sql(:all, repo, query)
    :erlang.phash2({sql, params})
  end

  defp with_read_context(repo, holder, source, params, fun) do
    previous = Process.get(@context_key)

    Process.put(@context_key, %{
      repo: repo,
      deps: [],
      holder: holder,
      source: source,
      params: params
    })

    try do
      fun.()
    after
      restore_read_context(previous)
    end
  end

  defp restore_read_context(nil), do: Process.delete(@context_key)
  defp restore_read_context(previous), do: Process.put(@context_key, previous)

  defp track_query(query) do
    case Process.get(@context_key) do
      %{deps: deps} = context ->
        deps = [Upkeep.Source.QueryDeps.from_query(query) | deps]
        Process.put(@context_key, %{context | deps: deps})

      _ ->
        raise "Upkeep.Source.Loader.track_query/1 called outside a source context. " <>
                "This usually means a Task spawned inside load/1 lost the context — " <>
                "explicit propagation is required for concurrent reads."
    end
  end

  defp emit_coverage(%Upkeep.Source.Coverage{} = coverage) do
    :telemetry.execute(
      [:upkeep, :source, :coverage],
      %{count: 1},
      %{
        source: coverage.source,
        params: coverage.params,
        coverage: coverage,
        severity: Upkeep.Source.Coverage.severity(coverage),
        known?: Upkeep.Source.Coverage.known?(coverage)
      }
    )
  end

  defp base_coverage(source, params) do
    explicit =
      if function_exported?(source, :__upkeep_explicit_interest_keys__, 1),
        do: source.__upkeep_explicit_interest_keys__(params),
        else: []

    Upkeep.Source.Coverage.new(source, params, explicit: explicit)
  end

  defp attach_unknown_if_empty(%Upkeep.Source.Coverage{} = coverage) do
    empty? =
      coverage.precise == [] and coverage.broad == [] and coverage.explicit == [] and
        coverage.unknown == []

    if empty? do
      %Upkeep.Source.Coverage{
        coverage
        | unknown: [%{reason: :no_invalidation_surface}]
      }
    else
      coverage
    end
  end

  defp warn_if_no_invalidation_surface(%Upkeep.Source.Coverage{unknown: []}), do: :ok

  defp warn_if_no_invalidation_surface(%Upkeep.Source.Coverage{} = coverage) do
    if Enum.any?(coverage.unknown, &(&1.reason == :no_invalidation_surface)) do
      shape = {coverage.source, coverage.params}
      seen = :persistent_term.get(@warn_dedup_key, MapSet.new())

      unless MapSet.member?(seen, shape) do
        :persistent_term.put(@warn_dedup_key, MapSet.put(seen, shape))

        require Logger

        Logger.warning(Upkeep.Source.Coverage.explain(coverage))
      end
    end

    :ok
  end

  defp tracked_deps do
    case Process.get(@context_key) do
      %{deps: deps} -> Enum.reverse(deps)
      _context -> []
    end
  end
end
