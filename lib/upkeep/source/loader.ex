defmodule Upkeep.Source.Loader do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Logger,
      Upkeep.InvalidationSurface,
      Upkeep.Source.Coverage,
      Upkeep.Source.Dependency,
      Upkeep.Source.Instance,
      Upkeep.Source.LoadResult
    ],
    type: :strict

  alias Upkeep.Source.{Coverage, Dependency, Instance, LoadResult}

  @context_key {__MODULE__, :read_context}
  @warn_dedup_key {__MODULE__, :no_invalidation_warned}

  @type read_context :: %{
          required(:repo) => module() | nil,
          required(:deps) => [term()],
          required(:holder) => term(),
          required(:source) => module(),
          required(:params) => map(),
          optional(:reads) => map()
        }

  @spec verify_source!(module(), map(), keyword()) :: :ok
  def verify_source!(source, params, opts \\ []) when is_atom(source) and is_map(params) do
    source
    |> Instance.build(params, current_scope: Keyword.get(opts, :current_scope))
    |> Instance.verify!(opts)
  end

  @spec load_result(module(), map()) :: LoadResult.t()
  def load_result(source, params) when is_atom(source) do
    source
    |> Instance.build(params)
    |> load_result()
  end

  @spec load_result(Instance.t()) :: LoadResult.t()
  def load_result(%Instance{} = instance) do
    Instance.verify!(instance, boundary: :load)

    {value, deps} = execute(instance)
    coverage = coverage(instance, deps)
    result = LoadResult.new(instance, value, deps, coverage)

    emit_coverage(result.coverage)
    warn_if_no_invalidation_surface(result.coverage)

    result
  end

  @spec read(term()) :: term()
  def read(value), do: value

  @spec coverage(module(), map()) :: Coverage.t()
  def coverage(source, params) when is_atom(source) and is_map(params) do
    source
    |> Instance.build(params)
    |> coverage()
  end

  @spec coverage(Instance.t(), [term()]) :: Coverage.t()
  def coverage(%Instance{} = instance, deps) when is_list(deps) do
    deps
    |> Enum.map(&Dependency.coverage/1)
    |> Enum.reduce(base_coverage(instance), &Coverage.merge/2)
    |> attach_unknown_if_empty()
  end

  @spec coverage(Instance.t()) :: Coverage.t()
  def coverage(%Instance{} = instance) do
    {_value, deps} = execute(instance)
    coverage(instance, deps)
  end

  @spec coverage(module(), map(), [term()]) :: Coverage.t()
  def coverage(source, params, deps) when is_atom(source) and is_map(params) and is_list(deps) do
    source
    |> Instance.build(params)
    |> coverage(deps)
  end

  @spec read_context() :: read_context() | nil
  def read_context, do: Process.get(@context_key)

  @spec track_dependency(term()) :: :ok | no_return()
  def track_dependency(deps) do
    case Process.get(@context_key) do
      %{deps: existing_deps} = context ->
        Process.put(@context_key, %{context | deps: [deps | existing_deps]})
        :ok

      _ ->
        raise "Upkeep.Source.Loader.track_dependency/1 called outside a source context. " <>
                "This usually means a Task spawned inside a load callback lost the context; " <>
                "explicit propagation is required for concurrent reads."
    end
  end

  @spec memoized_read(term(), (-> term())) :: term()
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
        value = load_value(instance)
        {value, tracked_deps()}
      end
    )
  end

  defp load_value(%Instance{
         identity_aware?: true,
         source: source,
         params: params,
         context: context
       }) do
    source.load(params, context)
  end

  defp load_value(%Instance{source: source, params: params}) do
    source.load(params)
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

  defp emit_coverage(%Coverage{} = coverage) do
    :telemetry.execute(
      [:upkeep, :source, :coverage],
      %{count: 1},
      %{
        source: coverage.source,
        params: coverage.params,
        coverage: coverage,
        severity: Coverage.severity(coverage),
        known?: Coverage.known?(coverage)
      }
    )
  end

  defp base_coverage(%Instance{} = instance) do
    Coverage.new(instance.source, instance.params,
      explicit: Upkeep.InvalidationSurface.keys(instance.explicit_surface)
    )
  end

  defp attach_unknown_if_empty(%Coverage{} = coverage) do
    empty? =
      coverage.precise == [] and coverage.broad == [] and coverage.explicit == [] and
        coverage.unknown == []

    if empty? do
      %Coverage{
        coverage
        | unknown: [%{reason: :no_invalidation_surface}]
      }
    else
      coverage
    end
  end

  defp warn_if_no_invalidation_surface(%Coverage{unknown: []}), do: :ok

  defp warn_if_no_invalidation_surface(%Coverage{} = coverage) do
    if Enum.any?(coverage.unknown, &(&1.reason == :no_invalidation_surface)) do
      shape = {coverage.source, coverage.params}
      seen = :persistent_term.get(@warn_dedup_key, MapSet.new())

      unless MapSet.member?(seen, shape) do
        :persistent_term.put(@warn_dedup_key, MapSet.put(seen, shape))

        require Logger

        Logger.warning(Coverage.explain(coverage))
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
