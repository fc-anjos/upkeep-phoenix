defprotocol Upkeep.Source.Dependency do
  @moduledoc false

  def coverage(deps)
  def surface(deps)
  def label(deps)
end
