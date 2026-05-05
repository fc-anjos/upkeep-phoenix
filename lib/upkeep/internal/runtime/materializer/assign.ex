defmodule Upkeep.Internal.Runtime.Materializer.Assign do
  @moduledoc false

  @enforce_keys [:assign_name, :node_id, :kind]
  defstruct [:assign_name, :node_id, :kind]
end
