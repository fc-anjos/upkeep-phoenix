defmodule Upkeep.Coordinator do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [Graph],
    deps: [
      Upkeep.Change,
      Upkeep.DAG,
      Upkeep.Invalidation,
      Upkeep.InvalidationSurface,
      Upkeep.Source,
      Upkeep.Source.Identity,
      Upkeep.Source.Instance,
      Upkeep.Source.Loader,
      Upkeep.Source.LoadResult,
      Upkeep.SingleFlight,
      Group
    ],
    type: :strict
end
