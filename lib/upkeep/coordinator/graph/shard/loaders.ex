defmodule Upkeep.Coordinator.Graph.Shard.Loaders do
  @moduledoc false

  alias Upkeep.Source.Instance
  alias Upkeep.Source.Loader, as: Source

  def run_with_deps(loader, metadata) when is_map(metadata) do
    :telemetry.execute(
      [:upkeep, :graph, :source_load, :start],
      %{system_time: System.system_time()},
      metadata
    )

    started_at = System.monotonic_time()
    loaded = run_with_deps(loader)
    duration = System.monotonic_time() - started_at

    :telemetry.execute(
      [:upkeep, :graph, :source_load, :stop],
      %{duration: duration},
      metadata
      |> Map.put(:surface_key_count, length(loaded.surface_keys))
      |> Map.put(:tracked_deps, length(loaded.tracked_deps))
    )

    loaded
  end

  def run_with_deps({:source, %Instance{} = instance}) do
    result = Source.load_result(instance)

    %{
      source_result: result,
      value: result.value,
      surface_keys: Upkeep.InvalidationSurface.keys(result.surface),
      tracked_deps: result.tracked_deps,
      surface: result.surface
    }
  end

  def run_with_deps({:fun, load_fn}) do
    {value, surface} = load_fn.()

    %{
      source_result: nil,
      value: value,
      surface_keys: Upkeep.InvalidationSurface.index_keys(surface),
      tracked_deps: [],
      surface: surface
    }
  end

  def metadata({:source, %Instance{} = instance}) do
    %{
      source: instance.source,
      params: instance.params,
      sharing_partition: instance.sharing_partition
    }
  end

  def metadata({:fun, _load_fn}), do: %{source: nil, params: nil}
  def metadata(nil), do: %{source: nil, params: nil}

  def retry_config({:source, %Instance{} = instance}), do: instance.retry
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
