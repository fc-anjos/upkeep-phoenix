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
      Upkeep.SingleFlight,
      Group
    ],
    type: :strict
end
