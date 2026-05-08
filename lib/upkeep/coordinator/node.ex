defmodule Upkeep.Coordinator.Node do
  @moduledoc false

  defstruct [
    :loader,
    :encoded_key,
    surface_keys: [],
    surface: nil,
    tracked_deps: [],
    loaded?: false,
    retry: :default
  ]
end
