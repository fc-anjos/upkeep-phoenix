defmodule Upkeep.Coordinator.Graph.Shard.Loaders do
  @moduledoc false

  alias Upkeep.Source.Identity, as: SourceIdentity
  alias Upkeep.Source.Loader, as: Source
  alias Upkeep.Source.Reactivity, as: SourceReactivity

  def run_with_deps(loader, metadata) when is_map(metadata) do
    :telemetry.execute(
      [:upkeep, :graph, :source_load, :start],
      %{system_time: System.system_time()},
      metadata
    )

    started_at = System.monotonic_time()
    {value, current_keys, tracked_deps} = run_with_deps(loader)
    duration = System.monotonic_time() - started_at

    :telemetry.execute(
      [:upkeep, :graph, :source_load, :stop],
      %{duration: duration},
      metadata
      |> Map.put(:registered_key_count, length(current_keys))
      |> Map.put(:tracked_deps, length(tracked_deps))
    )

    {value, current_keys, tracked_deps}
  end

  def run_with_deps({:source, source, params}) do
    {value, deps} = Source.load(source, params)
    dep_keys = SourceReactivity.deps_interest_keys(deps)
    {value, Enum.uniq(source.__upkeep_interest_keys__(params) ++ dep_keys), deps}
  end

  def run_with_deps({:fun, load_fn}) do
    {value, current_keys} = load_fn.()
    {value, current_keys, []}
  end

  def metadata({:source, source, params}) do
    %{
      source: source,
      params: params,
      sharing_partition: SourceIdentity.sharing_partition(source, params)
    }
  end

  def metadata({:fun, _load_fn}), do: %{source: nil, params: nil}
  def metadata(nil), do: %{source: nil, params: nil}

  def retry_config({:source, source, _params}), do: SourceIdentity.retry_config(source)
  def retry_config({:fun, _load_fn}), do: :default
  def retry_config(nil), do: :default

  def exception_metadata(loader, reason) do
    loader
    |> metadata()
    |> Map.merge(%{
      reason: reason,
      exception: exception(reason),
      stacktrace: stacktrace(reason)
    })
  end

  defp exception({%module{}, _stacktrace}), do: module
  defp exception(%module{}), do: module
  defp exception(reason), do: reason

  defp stacktrace({_exception, stacktrace}) when is_list(stacktrace), do: stacktrace
  defp stacktrace(_reason), do: []
end
