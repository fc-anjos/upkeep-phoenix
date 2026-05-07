defmodule Upkeep.Coordinator.Node do
  @moduledoc false

  defstruct [
    :loader,
    :encoded_key,
    registered_keys: [],
    reactive_surface: nil,
    tracked_deps: [],
    loaded?: false,
    retry: :default
  ]
end
