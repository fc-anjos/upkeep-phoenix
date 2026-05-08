defmodule Upkeep.Source.LoadResult do
  @moduledoc false

  alias Upkeep.Source.Instance

  @enforce_keys [:instance, :value, :tracked_deps, :surface, :coverage]
  defstruct instance: nil,
            value: nil,
            tracked_deps: [],
            surface: Upkeep.InvalidationSurface.empty(),
            coverage: nil

  @type t :: %__MODULE__{
          instance: Instance.t(),
          value: term(),
          tracked_deps: [term()],
          surface: Upkeep.InvalidationSurface.t(),
          coverage: Upkeep.Source.Coverage.t()
        }

  @spec new(Instance.t(), term(), [term()], Upkeep.Source.Coverage.t()) :: t()
  def new(%Instance{} = instance, value, tracked_deps, coverage) when is_list(tracked_deps) do
    %__MODULE__{
      instance: instance,
      value: value,
      tracked_deps: tracked_deps,
      surface: surface(instance, tracked_deps),
      coverage: coverage
    }
  end

  @spec keys(t()) :: [Upkeep.InvalidationSurface.key()]
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
