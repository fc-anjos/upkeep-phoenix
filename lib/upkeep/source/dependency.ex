defprotocol Upkeep.Source.Dependency do
  @moduledoc false

  def coverage(deps)
  def interest_keys(deps)
  def coarse_keys(deps)
  def matches_change?(deps, event)
  def label(deps)
end
