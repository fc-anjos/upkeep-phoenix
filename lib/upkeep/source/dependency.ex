defprotocol Upkeep.Source.Dependency do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [],
    type: :strict

  def coverage(deps)
  def surface(deps)
  def label(deps)
end
