defmodule Upkeep.DemoApp do
  @moduledoc false

  use Boundary,
    top_level?: true,
    check: [out: false]
end
