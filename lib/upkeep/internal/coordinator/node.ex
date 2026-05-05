defmodule Upkeep.Internal.Coordinator.Node do
  @moduledoc false

  defstruct [
    :loader,
    :encoded_key,
    registered_keys: [],
    tracked_deps: [],
    loaded?: false,
    retry: :default
  ]
end
