defmodule Upkeep.Coordinator.Graph.Shard.Loaders do
  @moduledoc false

  def run_with_deps({:source, source, params}) do
    {value, deps} = Upkeep.Source.load(source, params)
    dep_keys = Upkeep.Source.deps_interest_keys(deps)
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
      sharing_partition: Upkeep.Source.sharing_partition(source, params)
    }
  end

  def metadata({:fun, _load_fn}), do: %{source: nil, params: nil}
  def metadata(nil), do: %{source: nil, params: nil}

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
