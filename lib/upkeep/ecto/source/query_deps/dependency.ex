defimpl Upkeep.Source.Dependency, for: Upkeep.Ecto.Source.QueryDeps do
  @moduledoc false

  def coverage(deps), do: Upkeep.Ecto.Source.QueryDeps.coverage(deps)
  def surface(deps), do: Upkeep.Ecto.Source.QueryDeps.surface(deps)
  def label(deps), do: Upkeep.Ecto.Source.QueryDeps.label(deps)
end
