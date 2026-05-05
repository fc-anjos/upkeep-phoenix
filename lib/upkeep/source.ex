defmodule Upkeep.Source do
  @moduledoc """
  Source authoring helpers for Upkeep-style reactive reads.

  A source is a module that can load a live read. Ecto-backed sources should
  perform database reads through `Upkeep.read/1`; those reads are tracked as
  the source's reactive surface.
  """

  use Boundary,
    exports: [
      Coverage,
      Identity,
      Loader,
      Reactivity,
      ReadCache
    ],
    deps: [
      Upkeep,
      Upkeep.SingleFlight,
      Ecto.Adapters.SQL,
      Ecto.Query,
      Ecto.Queryable,
      Ecto.SubQuery,
      Logger,
      {Mix, :compile}
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

  defdelegate read(query_or_value), to: Upkeep.Source.Loader
  defdelegate coverage(source, params), to: Upkeep.Source.Loader
  defdelegate coverage(source, params, deps), to: Upkeep.Source.Loader
end
