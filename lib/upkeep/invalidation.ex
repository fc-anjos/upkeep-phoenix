defmodule Upkeep.Invalidation do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [
      SourceInvalidator
    ],
    deps: [
      Group,
      Upkeep.Change,
      Upkeep.Coordinator,
      Upkeep.Source
    ],
    type: :strict

  def dispatch([]), do: :ok

  def dispatch(events) when is_list(events) do
    Enum.each(events, &dispatch_one/1)
    :ok
  end

  def dispatch(event) when is_struct(event), do: dispatch([event])

  def reset do
    Upkeep.Source.ReadCache.clear()
  end

  defp dispatch_one(event) do
    Upkeep.Change.diagnose_broad_update(event)
    Upkeep.Source.ReadCache.invalidate(event)
    Upkeep.Coordinator.Graph.notify(event)
  end
end
