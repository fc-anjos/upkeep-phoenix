defmodule Upkeep.Coordinator do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [Graph],
    deps: [
      Upkeep.Change,
      Upkeep.DAG,
      Upkeep.Source,
      Upkeep.SingleFlight,
      Group
    ],
    type: :strict
end
