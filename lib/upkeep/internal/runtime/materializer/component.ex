defmodule Upkeep.Internal.Runtime.Materializer.Component do
  @moduledoc false

  @enforce_keys [:component_id, :node_id]
  defstruct [:component_id, :node_id]
end
