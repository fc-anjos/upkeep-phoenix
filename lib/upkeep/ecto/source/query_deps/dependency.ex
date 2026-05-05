defimpl Upkeep.Source.Dependency, for: Upkeep.Ecto.Source.QueryDeps do
  @moduledoc false

  def coverage(deps), do: Upkeep.Ecto.Source.QueryDeps.coverage(deps)
  def interest_keys(deps), do: Upkeep.Ecto.Source.QueryDeps.interest_keys(deps)
  def coarse_keys(deps), do: Upkeep.Ecto.Source.QueryDeps.coarse_keys(deps)
  def matches_change?(deps, event), do: Upkeep.Ecto.Source.QueryDeps.matches_change?(deps, event)
  def label(deps), do: Upkeep.Ecto.Source.QueryDeps.label(deps)
end
