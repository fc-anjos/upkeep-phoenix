defmodule Upkeep.Source.Loader do
  @moduledoc false

  alias Upkeep.Source.{Instance, LoadResult}

  @context_key {__MODULE__, :read_context}
  @warn_dedup_key {__MODULE__, :no_invalidation_warned}

  def verify_source!(source, params, opts \\ []) when is_atom(source) and is_map(params) do
    source
    |> Instance.build(params)
    |> Instance.verify!(opts)
  end

  def load_result(source, params) when is_atom(source) do
    source
    |> Instance.build(params)
    |> load_result()
  end

  def load_result(%Instance{} = instance) do
    {value, deps} = execute(instance)
    coverage = coverage(instance, deps)
    result = LoadResult.new(instance, value, deps, coverage)

    emit_coverage(result.coverage)
    warn_if_no_invalidation_surface(result.coverage)

    result
  end

  def read(value), do: value

  def coverage(source, params) when is_atom(source) and is_map(params) do
    source
    |> Instance.build(params)
    |> coverage()
  end

  def coverage(%Instance{} = instance, deps) when is_list(deps) do
    deps
    |> Enum.map(&Upkeep.Source.Dependency.coverage/1)
    |> Enum.reduce(base_coverage(instance), &Upkeep.Source.Coverage.merge/2)
    |> attach_unknown_if_empty()
  end

  def coverage(%Instance{} = instance) do
    {_value, deps} = execute(instance)
    coverage(instance, deps)
  end

  def coverage(source, params, deps) when is_atom(source) and is_map(params) and is_list(deps) do
    source
    |> Instance.build(params)
    |> coverage(deps)
  end

  def read_context, do: Process.get(@context_key)

  def track_dependency(deps) do
    case Process.get(@context_key) do
      %{deps: existing_deps} = context ->
        Process.put(@context_key, %{context | deps: [deps | existing_deps]})

      _ ->
        raise "Upkeep.Source.Loader.track_dependency/1 called outside a source context. " <>
                "This usually means a Task spawned inside load/1 lost the context — " <>
                "explicit propagation is required for concurrent reads."
    end
  end

  def memoized_read(fingerprint, read) when is_function(read, 0) do
    cache = Map.get(Process.get(@context_key), :reads, %{})

    case Map.fetch(cache, fingerprint) do
      {:ok, value} ->
        value

      :error ->
        value = read.()
        ctx = Process.get(@context_key)
        Process.put(@context_key, Map.put(ctx, :reads, Map.put(cache, fingerprint, value)))
        value
    end
  end

  defp execute(%Instance{} = instance) do
    with_read_context(
      instance.repo,
      instance.id,
      instance.source,
      instance.params,
      fn ->
        value = instance.source.load(instance.params)
        {value, tracked_deps()}
      end
    )
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

  defp base_coverage(%Instance{} = instance) do
    Upkeep.Source.Coverage.new(instance.source, instance.params,
      explicit: Upkeep.InvalidationSurface.keys(instance.explicit_surface)
    )
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
