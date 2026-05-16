defmodule Upkeep.Coordinator.SourceLoader do
  @moduledoc false

  alias Upkeep.Coordinator.{LoadedSource, Node}
  alias Upkeep.Source.Instance
  alias Upkeep.Source.Loader, as: Source

  def run_with_deps(node_id, %Node{} = node, metadata) when is_map(metadata) do
    :telemetry.execute(
      [:upkeep, :graph, :source_load, :start],
      %{system_time: System.system_time()},
      metadata
    )

    started_at = System.monotonic_time()
    loaded = run_with_deps(node_id, node)
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

  def run_with_deps(node_id, %Node{loader: {:source, %Instance{} = instance}} = node) do
    result = Source.load_result(instance)
    LoadedSource.from_source_result(node_id, node, result)
  end

  def run_with_deps(node_id, %Node{loader: {:fun, load_fn}} = node) do
    {value, surface} = load_fn.()
    LoadedSource.from_fun(node_id, node, value, surface)
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
