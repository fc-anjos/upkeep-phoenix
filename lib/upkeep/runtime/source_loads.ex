defmodule Upkeep.Runtime.SourceLoads do
  @moduledoc false

  alias Upkeep.Runtime.Telemetry
  alias Upkeep.SingleFlight.Registry
  alias Upkeep.Source.Instance
  alias Upkeep.Source.Loader, as: Source
  alias Upkeep.Source.LoadResult

  def load(watch, reason) do
    load(watch.instance, watch.source_id, watch.component, reason)
  end

  @doc """
  Load a source without letting a raising/exiting loader take down the caller.

  Returns `{:ok, result}` on success or `{:error, info}` on failure. The
  `[:upkeep, :source, :reload]` span still emits its `:exception` event before
  this rescues, so observability is unchanged — only the LiveView no longer
  crashes. Used by the mount and local-refresh paths, which run the loader
  directly on the LiveView process.
  """
  def safe_load(watch, reason) do
    {:ok, load(watch, reason)}
  rescue
    error -> rescue_load(error, __STACKTRACE__)
  catch
    kind, value -> {:error, %{kind: kind, error: value, stacktrace: __STACKTRACE__}}
  end

  def safe_load_coalesced(%Instance{} = instance, source_id, component, reason) do
    {:ok, load_coalesced(instance, source_id, component, reason)}
  rescue
    error -> rescue_load(error, __STACKTRACE__)
  catch
    kind, value -> {:error, %{kind: kind, error: value, stacktrace: __STACKTRACE__}}
  end

  # A missing `current_scope` or other misconfiguration surfaces as an
  # `ArgumentError`. That is a developer/setup mistake (not a transient source
  # failure), so it must keep crashing loudly rather than degrading to a silent
  # error state.
  defp rescue_load(%ArgumentError{} = error, stacktrace) do
    reraise(error, stacktrace)
  end

  defp rescue_load(error, stacktrace) do
    {:error, %{kind: :error, error: error, stacktrace: stacktrace}}
  end

  @doc """
  Build a degraded `LoadResult` for a source whose load failed.

  The value is `nil`, but the surface falls back to the source's declared
  (`invalidated_by`) surface so the watch stays reactive: a later matching
  invalidation can refresh it and recover, rather than the watch being inert.
  """
  def degraded_result(%Instance{} = instance) do
    %LoadResult{
      instance: instance,
      value: nil,
      tracked_deps: [],
      surface: instance.explicit_surface,
      coverage: nil
    }
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
