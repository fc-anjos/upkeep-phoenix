defmodule Upkeep.Source do
  @moduledoc """
  Source authoring helpers for Upkeep-style reactive reads.

  A source is a module that can load a live read. Ecto-backed sources use
  `Upkeep.Ecto.Source`, which plugs Ecto query analysis into this generic
  source contract.
  """

  use Boundary,
    top_level?: true,
    exports: [
      Coverage,
      Dependency,
      Identity,
      Instance,
      LoadResult,
      Loader
    ],
    deps: [
      Upkeep.Change,
      Upkeep.InvalidationSurface,
      Logger
    ],
    type: :strict

  defmacro __using__(opts) do
    quote do
      use Upkeep.Source.Spec, unquote(opts)
    end
  end

  defmacro invalidated_by(notification, opts) do
    Upkeep.Source.Spec.invalidated_by_definition(notification, opts, __CALLER__)
  end

  defmacro invalidated_by(schema, action, opts) do
    Upkeep.Source.Spec.invalidated_by_definition(schema, action, opts, __CALLER__)
  end

  defmacro reacts_to(notification, fun) do
    Upkeep.Source.Spec.reacts_to_definition(notification, fun, __CALLER__)
  end

  defmacro reacts_to(schema, action, fun) do
    Upkeep.Source.Spec.reacts_to_definition(schema, action, fun, __CALLER__)
  end

  @doc false
  def read(value), do: Upkeep.Source.Loader.read(value)

  @doc """
  Return Upkeep's inferred invalidation coverage for a source and params.
  """
  def coverage(source, params), do: Upkeep.Source.Loader.coverage(source, params)

  @doc false
  def coverage(source, params, deps), do: Upkeep.Source.Loader.coverage(source, params, deps)

  @doc false
  def instance(source, params), do: Upkeep.Source.Instance.build(source, params)

  @doc false
  def dependency_label(deps), do: Upkeep.Source.Dependency.label(deps)

  @doc false
  def dependency_surface([]), do: Upkeep.InvalidationSurface.empty()

  @doc false
  def dependency_surface(deps) when is_list(deps) do
    deps
    |> Enum.map(&Upkeep.Source.Dependency.surface/1)
    |> Upkeep.InvalidationSurface.merge_all()
  end
end
