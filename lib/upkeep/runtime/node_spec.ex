defmodule Upkeep.Runtime.NodeSpec do
  @moduledoc false

  @enforce_keys [:id, :kind, :deps, :producer, :scope, :materializers, :metadata]
  defstruct [:id, :kind, :deps, :producer, :scope, :materializers, :metadata]
end
