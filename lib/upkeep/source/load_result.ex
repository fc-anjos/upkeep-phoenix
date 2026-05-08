defmodule Upkeep.Source.LoadResult do
  @moduledoc false

  alias Upkeep.Source.Instance

  @enforce_keys [:instance, :value, :tracked_deps, :surface, :coverage]
  defstruct instance: nil,
            value: nil,
            tracked_deps: [],
            surface: Upkeep.InvalidationSurface.empty(),
            coverage: nil

  def new(%Instance{} = instance, value, tracked_deps, coverage) when is_list(tracked_deps) do
    %__MODULE__{
      instance: instance,
      value: value,
      tracked_deps: tracked_deps,
      surface: surface(instance, tracked_deps),
      coverage: coverage
    }
  end

  def keys(%__MODULE__{surface: surface}) do
    Upkeep.InvalidationSurface.keys(surface)
  end

  defp surface(%Instance{} = instance, tracked_deps) do
    Upkeep.InvalidationSurface.merge(
      instance.surface,
      Upkeep.Source.dependency_surface(tracked_deps)
    )
  end
end
