defmodule Upkeep.Coordinator do
  @moduledoc false

  use Boundary,
    exports: [Graph],
    deps: [
      Upkeep,
      Upkeep.DAG,
      Upkeep.Source,
      Upkeep.SingleFlight,
      Group
    ],
    type: :strict
end
