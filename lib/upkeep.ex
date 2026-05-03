defmodule Upkeep do
  @moduledoc """
  Domain-reactive LiveView runtime prototype.
  """

  defdelegate mutate(fun), to: Upkeep.Mutation
  defdelegate mutate(repo, fun), to: Upkeep.Mutation
  defdelegate notify(event), to: Upkeep.Mutation
end
