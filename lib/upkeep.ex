defmodule Upkeep do
  @moduledoc """
  Domain-reactive LiveView runtime.
  """

  defdelegate mutate(fun), to: Upkeep.Mutation
  defdelegate mutate(repo, fun), to: Upkeep.Mutation
  defdelegate notify(event), to: Upkeep.Mutation
  defdelegate changed(name, payload, opts \\ []), to: Upkeep.Mutation
  defdelegate inserted(record, opts \\ []), to: Upkeep.Mutation
  defdelegate updated(record, opts \\ []), to: Upkeep.Mutation
  defdelegate deleted(record, opts \\ []), to: Upkeep.Mutation
  defdelegate read(query), to: Upkeep.Source
  defdelegate recent_events(opts \\ []), to: Upkeep.Observability, as: :recent
  defdelegate clear_events(), to: Upkeep.Observability, as: :clear
end
