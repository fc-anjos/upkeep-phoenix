defmodule Upkeep.Runtime.SourceLoads do
  @moduledoc false

  alias Upkeep.Live.Telemetry
  alias Upkeep.Runtime.Subscriptions
  alias Upkeep.Source.Loader, as: Source
  alias Upkeep.Source.Reactivity, as: SourceReactivity
  alias Upkeep.SingleFlight.Registry

  def load(watch, reason) do
    load(watch.source, watch.params, watch.source_id, watch.component, reason)
  end

  def load(source, params, source_id, component, reason) do
    Telemetry.span(
      [:source, :reload],
      Telemetry.source_metadata(source, params, source_id, component, reason),
      fn ->
        {value, tracked_deps} = Source.load(source, params)
        {{value, tracked_deps}, %{changed?: nil, tracked_deps: length(tracked_deps)}}
      end
    )
  end

  def load_coalesced(source, params, source_id, component, reason) do
    if Process.whereis(coalescer_name()) do
      Registry.coalesce(coalescer_name(), source_id, fn ->
        emit_initial_load_miss(source, params, source_id, component)
        load(source, params, source_id, component, reason)
      end)
    else
      load(source, params, source_id, component, reason)
    end
  end

  def coalescer_name, do: __MODULE__.Coalescer

  defp emit_initial_load_miss(source, params, source_id, component) do
    :telemetry.execute(
      [:upkeep, :source, :initial_load, :miss],
      %{count: 1},
      Telemetry.source_metadata(source, params, source_id, component, :initial_load)
    )
  end

  def load_or_register(_socket, true, source_id, source, params, _component) do
    static_keys = source.__upkeep_interest_keys__(params)

    {:ok, value, tracked_deps} =
      Subscriptions.register_and_load(source_id, static_keys, source, params)

    interest_keys = interest_keys(source, params, tracked_deps)

    {value, tracked_deps, interest_keys}
  end

  def load_or_register(_socket, false, source_id, source, params, component) do
    {value, tracked_deps} = load(source, params, source_id, component, :watch)
    {value, tracked_deps, interest_keys(source, params, tracked_deps)}
  end

  def update_watch_deps(watch, tracked_deps) do
    %{
      watch
      | interest_keys: interest_keys(watch.source, watch.params, tracked_deps),
        tracked_deps: tracked_deps
    }
  end

  def reacts_to?(watch, event) do
    SourceReactivity.deps_react_to?(Map.get(watch, :tracked_deps, []), event) or
      watch.source.reacts_to?(event, watch.params)
  end

  def interest_keys(source, params, tracked_deps) do
    (source.__upkeep_interest_keys__(params) ++ SourceReactivity.deps_interest_keys(tracked_deps))
    |> Enum.uniq()
  end
end
