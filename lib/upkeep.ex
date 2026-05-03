defmodule Upkeep do
  @moduledoc """
  Domain-reactive LiveView runtime prototype.
  """

  defdelegate mutate(fun), to: Upkeep.Mutation
  defdelegate mutate(repo, fun), to: Upkeep.Mutation
  defdelegate notify(event), to: Upkeep.Mutation
  defdelegate changed(name, payload, opts \\ []), to: Upkeep.Mutation
  defdelegate inserted(record, opts \\ []), to: Upkeep.Mutation
  defdelegate updated(record, opts \\ []), to: Upkeep.Mutation
  defdelegate deleted(record, opts \\ []), to: Upkeep.Mutation
end
