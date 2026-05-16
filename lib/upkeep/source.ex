defmodule Upkeep.Source do
  @moduledoc """
  Source authoring helpers for Upkeep-style reactive reads.

  A source is a module that can load a live read. Ecto-backed sources use
  `Upkeep.Ecto.Source`, which plugs Ecto query analysis into this generic
  source contract.
  """

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Upkeep.InvalidationSurface,
      Upkeep.Source.Context,
      Upkeep.Source.Loader
    ],
    type: :strict

  alias Upkeep.Source.{Context, Loader, Spec}

  @type params :: map()
  @type source_module :: module()
  @type retry_config :: :default | false | keyword()
  @type idle_ttl_config :: nil | :infinity | non_neg_integer()
  @type source_context :: term()

  @callback load(params()) :: term()
  @callback load(params(), source_context()) :: term()
  @callback query(params()) :: term()
  @callback query(params(), source_context()) :: term()
  @callback reacts_to?(struct(), params()) :: boolean()
  @callback __upkeep_repo__() :: module() | nil
  @callback __upkeep_repo_explicit__?() :: boolean()
  @callback __upkeep_query_source__?() :: boolean()
  @callback __upkeep_identity_aware__?() :: boolean()
  @callback __upkeep_retry__() :: retry_config()
  @callback __upkeep_idle_ttl_ms__() :: idle_ttl_config()
  @callback __upkeep_sharing_partition__(params()) :: term()
  @callback __upkeep_verify__!(params(), keyword()) :: :ok
  @callback __upkeep_surface__(params()) :: Upkeep.InvalidationSurface.t()
  @callback __upkeep_surface__(params(), Context.t() | nil) :: Upkeep.InvalidationSurface.t()
  @callback __upkeep_explicit_surface__(params()) :: Upkeep.InvalidationSurface.t()
  @callback __upkeep_explicit_surface_matches__?(params(), struct()) :: boolean()

  @optional_callbacks load: 1,
                      load: 2,
                      query: 1,
                      query: 2,
                      reacts_to?: 2,
                      __upkeep_repo__: 0,
                      __upkeep_repo_explicit__?: 0,
                      __upkeep_query_source__?: 0,
                      __upkeep_identity_aware__?: 0,
                      __upkeep_retry__: 0,
                      __upkeep_idle_ttl_ms__: 0,
                      __upkeep_sharing_partition__: 1,
                      __upkeep_verify__!: 2,
                      __upkeep_surface__: 1,
                      __upkeep_surface__: 2,
                      __upkeep_explicit_surface__: 1,
                      __upkeep_explicit_surface_matches__?: 2

  defmacro __using__(opts) do
    quote do
      use unquote(Spec), unquote(opts)
    end
  end

  defmacro invalidated_by(notification, opts) do
    Spec.invalidated_by_definition(notification, opts, __CALLER__)
  end

  defmacro invalidated_by(schema, action, opts) do
    Spec.invalidated_by_definition(schema, action, opts, __CALLER__)
  end

  defmacro reacts_to(notification, fun) do
    Spec.reacts_to_definition(notification, fun, __CALLER__)
  end

  defmacro reacts_to(schema, action, fun) do
    Spec.reacts_to_definition(schema, action, fun, __CALLER__)
  end

  @doc """
  Return the Phoenix `:current_scope` value available to an identity-aware
  source callback.
  """
  def current_scope!(%Context{} = context), do: Context.current_scope!(context)

  @doc """
  Return Upkeep's inferred invalidation coverage for a source and params.
  """
  def coverage(source, params), do: Loader.coverage(source, params)
end
