defmodule Upkeep.Runtime.SourceLoads do
  @moduledoc false

  alias Upkeep.Runtime.Telemetry
  alias Upkeep.Source.Instance
  alias Upkeep.Source.LoadResult
  alias Upkeep.Source.Loader, as: Source
  alias Upkeep.SingleFlight.Registry

  def load(watch, reason) do
    load(watch.instance, watch.source_id, watch.component, reason)
  end

  def load(%Instance{} = instance, source_id, component, reason) do
    Telemetry.span(
      [:source, :reload],
      Telemetry.source_metadata(instance, source_id, component, reason),
      fn ->
        result = Source.load_result(instance)

        {result,
         %{
           changed?: nil,
           tracked_deps: length(result.tracked_deps),
           surface_keys: length(Upkeep.InvalidationSurface.keys(result.surface))
         }}
      end
    )
  end

  def load_coalesced(%Instance{} = instance, source_id, component, reason) do
    if Process.whereis(coalescer_name()) do
      Registry.coalesce(coalescer_name(), source_id, fn ->
        emit_initial_load_miss(instance, source_id, component)
        load(instance, source_id, component, reason)
      end)
    else
      load(instance, source_id, component, reason)
    end
  end

  def coalescer_name, do: __MODULE__.Coalescer

  defp emit_initial_load_miss(%Instance{} = instance, source_id, component) do
    :telemetry.execute(
      [:upkeep, :source, :initial_load, :miss],
      %{count: 1},
      Telemetry.source_metadata(instance, source_id, component, :initial_load)
    )
  end

  def update_watch_result(watch, %LoadResult{} = result) do
    %{
      watch
      | surface: result.surface,
        tracked_deps: result.tracked_deps
    }
  end

  def reacts_to?(watch, event) do
    Upkeep.InvalidationSurface.matches?(watch.surface, event)
  end
end
