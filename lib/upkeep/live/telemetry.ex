defmodule Upkeep.Live.Telemetry do
  @moduledoc false

  alias Upkeep.Live.Ids

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

  def watch_metadata(watch, assign_name, kind) do
    %{
      source_id: watch.source_id,
      node_id: Ids.source_node_id(watch.source_id),
      source: watch.source,
      params: watch.params,
      component: watch.component,
      assign_name: assign_name,
      assign_names: watch.assign_names |> MapSet.to_list() |> sort_terms(),
      kind: kind,
      registered?: watch.registered?,
      interest_keys: watch.interest_keys
    }
  end

  def source_metadata(source, params, source_id, component, reason) do
    %{
      source_id: source_id,
      node_id: Ids.source_node_id(source_id),
      source: source,
      params: params,
      component: component,
      reason: reason
    }
  end

  def sort_terms(terms), do: Enum.sort_by(terms, &inspect/1)
end
