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
      Keys,
      Loader,
      Reactivity,
      ReadCache
    ],
    deps: [
      Upkeep.Change,
      Upkeep.SingleFlight,
      Logger
    ],
    type: :strict

  defmacro __using__(opts) do
    quote do
      use Upkeep.Source.Spec, unquote(opts)
    end
  end

  defmacro query(fun) do
    Upkeep.Source.Spec.query_definition(fun)
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

  def read(value), do: Upkeep.Source.Loader.read(value)

  def coverage(source, params), do: Upkeep.Source.Loader.coverage(source, params)

  def coverage(source, params, deps), do: Upkeep.Source.Loader.coverage(source, params, deps)
end
