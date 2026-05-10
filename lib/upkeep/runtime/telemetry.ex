defmodule Upkeep.Runtime.Telemetry do
  @moduledoc false

  alias Upkeep.Runtime.Ids
  alias Upkeep.Source.Instance

  def emit(event, measurements, metadata) do
    :telemetry.execute([:upkeep | event], measurements, metadata)
  end

  def span(event, metadata, fun) do
    :telemetry.span([:upkeep | event], metadata, fn ->
      {result, stop_metadata} = fun.()
      {result, Map.merge(metadata, stop_metadata)}
    end)
  end

  def watch_metadata(watch), do: watch_metadata(watch, nil, :remove)

  def watch_metadata(watch, opts) when is_list(opts) do
    Map.merge(watch_metadata(watch), Map.new(opts))
  end

  def watch_alias_metadata(watch, assign_name) when is_atom(assign_name) do
    instance = watch.instance

    %{
      source_id: watch.source_id,
      node_id: Ids.source_node_id(watch.source_id),
      source: instance.source,
      params: instance.params,
      sharing_partition: instance.sharing_partition,
      component: watch.component,
      assign_name: assign_name,
      assign_count: MapSet.size(watch.assign_names),
      kind: :alias,
      registered?: watch.registered?,
      surface_keys: Upkeep.InvalidationSurface.keys(watch.surface)
    }
  end

  def watch_metadata(watch, assign_name, kind) do
    instance = watch.instance

    %{
      source_id: watch.source_id,
      node_id: Ids.source_node_id(watch.source_id),
      source: instance.source,
      params: instance.params,
      sharing_partition: instance.sharing_partition,
      component: watch.component,
      assign_name: assign_name,
      assign_names: watch.assign_names |> MapSet.to_list() |> sort_terms(),
      kind: kind,
      registered?: watch.registered?,
      surface_keys: Upkeep.InvalidationSurface.keys(watch.surface)
    }
  end

  def source_metadata(%Instance{} = instance, source_id, component, reason) do
    %{
      source_id: source_id,
      node_id: Ids.source_node_id(source_id),
      source: instance.source,
      params: instance.params,
      sharing_partition: instance.sharing_partition,
      component: component,
      reason: reason
    }
  end

  def sort_terms(terms), do: Enum.sort_by(terms, &inspect/1)
end
