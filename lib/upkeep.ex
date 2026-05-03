defmodule Upkeep do
  @moduledoc """
  Domain-reactive LiveView runtime prototype.
  """

  defdelegate notify(event), to: Upkeep.Coordinator
end
