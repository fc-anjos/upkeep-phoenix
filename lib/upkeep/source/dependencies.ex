defmodule Upkeep.Source.Dependencies do
  @moduledoc false

  alias Upkeep.Source.Dependency

  @spec surface([term()]) :: Upkeep.InvalidationSurface.t()
  def surface([]), do: Upkeep.InvalidationSurface.empty()

  def surface(dependencies) when is_list(dependencies) do
    dependencies
    |> Enum.map(&Dependency.surface/1)
    |> Upkeep.InvalidationSurface.merge_all()
  end
end
