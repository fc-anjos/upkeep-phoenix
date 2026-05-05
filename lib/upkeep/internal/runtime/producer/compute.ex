defmodule Upkeep.Internal.Runtime.Producer.Compute do
  @moduledoc false

  @enforce_keys [:deps, :dep_pairs, :fun]
  defstruct [:deps, :dep_pairs, :fun, :identity]
end
