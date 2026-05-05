defmodule Upkeep.Source.Reactivity do
  @moduledoc false

  def event_keys(event), do: Upkeep.Source.Keys.event_keys(event)

  def deps_interest_keys(deps) do
    deps
    |> Enum.flat_map(&Upkeep.Source.Dependency.interest_keys/1)
    |> Enum.uniq()
  end

  def deps_react_to?(deps, event) do
    Enum.any?(deps, &Upkeep.Source.Dependency.matches_change?(&1, event))
  end
end
