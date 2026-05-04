defmodule Upkeep.Coordinator.Node do
  @moduledoc false

  defstruct [
    :id,
    :kind,
    :loader,
    :encoded_key,
    registered_keys: [],
    tracked_deps: [],
    loaded?: false
  ]
end
