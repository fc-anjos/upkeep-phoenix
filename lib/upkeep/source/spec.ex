defmodule Upkeep.Source.Spec do
  @moduledoc false

  defmacro __using__(opts) do
    repo =
      opts
      |> Keyword.get(:repo)
      |> Macro.expand(__CALLER__)

    retry = Keyword.get(opts, :retry, :default)
    query_adapter = Keyword.get(opts, :query_adapter)

    quote bind_quoted: [repo: repo, retry: retry, query_adapter: query_adapter] do
      @behaviour Upkeep.Source

      import Upkeep.Source,
        only: [invalidated_by: 2, invalidated_by: 3, reacts_to: 2, reacts_to: 3]

      @upkeep_repo repo
      @upkeep_retry retry
      @upkeep_query_adapter query_adapter
      Module.register_attribute(__MODULE__, :upkeep_invalidators, accumulate: true)
      Module.register_attribute(__MODULE__, :upkeep_reactors, accumulate: true)
      @before_compile Upkeep.Source.Spec
    end
  end

  def invalidated_by_definition(notification, opts, caller) do
    build_invalidated_by(normalize_notification(notification, caller), opts)
  end

  def invalidated_by_definition(schema, action, opts, caller) do
    notification = normalize_notification(schema, action, caller)
    build_invalidated_by(notification, opts)
  end

  def reacts_to_definition(notification, fun, caller) do
    notification = normalize_notification(notification, caller)

    quote bind_quoted: [notification: Macro.escape(notification), fun: Macro.escape(fun)] do
      @upkeep_reactors {notification, fun}
    end
  end

  def reacts_to_definition(schema, action, fun, caller) do
    notification = normalize_notification(schema, action, caller)

    quote bind_quoted: [notification: Macro.escape(notification), fun: Macro.escape(fun)] do
      @upkeep_reactors {notification, fun}
    end
  end

  defmacro __before_compile__(env) do
    context = before_compile_context(env)
    validate_query_source!(context)

    quote_source_definition(context)
  end

  defp before_compile_context(env) do
    invalidators = Module.get_attribute(env.module, :upkeep_invalidators)
    reactors = Module.get_attribute(env.module, :upkeep_reactors)

    %{
      module: env.module,
      invalidators: invalidators,
      reactors: reactors,
      repo: Module.get_attribute(env.module, :upkeep_repo),
      retry: Module.get_attribute(env.module, :upkeep_retry),
      query_adapter: Module.get_attribute(env.module, :upkeep_query_adapter),
      defines_load_1?: Module.defines?(env.module, {:load, 1}),
      defines_load_2?: Module.defines?(env.module, {:load, 2}),
      defines_query_1?: Module.defines?(env.module, {:query, 1}),
      defines_query_2?: Module.defines?(env.module, {:query, 2}),
      defines_sharing_partition?: Module.defines?(env.module, {:__upkeep_sharing_partition__, 1})
    }
  end

  defp validate_query_source!(%{
         defines_query_1?: query_1?,
         defines_query_2?: query_2?,
         query_adapter: nil,
         module: module
       })
       when query_1? or query_2? do
    raise ArgumentError,
          "#{inspect(module)} defines query/1 or query/2 but uses Upkeep.Source. " <>
            "Use load/1 or load/2 for generic sources or use Upkeep.Ecto.Source for Ecto-backed query sources."
  end

  defp validate_query_source!(_context), do: :ok

  defp quote_source_definition(context) do
    definition = source_definition(context)

    quote do
      unquote(load_definition(context))
      unquote(sharing_definition(context, definition))
      unquote(verify_definition(context.query_adapter))
      unquote(source_fact_definitions(context, definition))
      unquote(surface_definitions(context, definition))
    end
  end

  defp source_fact_definitions(context, definition) do
    quote do
      def __upkeep_repo__, do: unquote(context.repo)
      def __upkeep_repo_explicit__?, do: unquote(not is_nil(context.repo))
      def __upkeep_query_source__?, do: unquote(definition.query_source?)
      def __upkeep_identity_aware__?, do: unquote(definition.identity_aware?)
      def __upkeep_retry__, do: unquote(Macro.escape(context.retry))
    end
  end

  defp surface_definitions(context, definition) do
    quote do
      def reacts_to?(event, params),
        do:
          unquote(
            reacts_to_body(
              definition.explicit_checks,
              definition.query_source?,
              context.query_adapter
            )
          )

      def __upkeep_explicit_surface_matches__?(params, event),
        do: unquote(explicit_match_body(definition.explicit_checks))

      def __upkeep_surface__(params) do
        __upkeep_surface__(params, nil)
      end

      def __upkeep_surface__(params, context) do
        Upkeep.InvalidationSurface.merge(
          __upkeep_explicit_surface__(params),
          unquote(
            query_surface(
              definition.query_source?,
              context.query_adapter
            )
          )
        )
      end

      def __upkeep_explicit_surface__(params) do
        unquote(explicit_surface(definition.explicit_keys))
      end
    end
  end

  defp sharing_definition(context, definition) do
    sharing_partition_definition(
      context.defines_sharing_partition?,
      definition.partition_fields
    )
  end

  defp source_definition(context) do
    invalidator_checks = invalidator_checks(context.invalidators)
    reactor_checks = reactor_checks(context.reactors)

    %{
      partition_fields: partition_fields(context.invalidators),
      explicit_checks: invalidator_checks ++ reactor_checks,
      explicit_keys: explicit_keys(context.invalidators, context.reactors),
      query_source?: query_source?(context),
      identity_aware?: identity_aware?(context)
    }
  end

  defp query_source?(%{defines_query_1?: query_1?, defines_query_2?: query_2?}) do
    query_1? or query_2?
  end

  defp identity_aware?(%{defines_load_2?: load_2?, defines_query_2?: query_2?}) do
    load_2? or query_2?
  end

  defp partition_fields(invalidators) do
    invalidators
    |> Enum.flat_map(fn {_notification, _on, as} -> as end)
    |> Enum.uniq()
  end

  defp invalidator_checks(invalidators) do
    Enum.map(invalidators, fn {notification, on, as} ->
      quote do
        Upkeep.InvalidationSurface.matches_notification?(
          event,
          unquote(Macro.escape(notification))
        ) and
          Upkeep.InvalidationSurface.equal_fields?(event, params, unquote(on), unquote(as))
      end
    end)
  end

  defp reactor_checks(reactors) do
    Enum.map(reactors, fn {notification, fun} ->
      quote do
        Upkeep.InvalidationSurface.matches_notification?(
          event,
          unquote(Macro.escape(notification))
        ) and
          (Upkeep.Change.broad_update?(event) or Upkeep.Change.partial_update?(event) or
             unquote(fun).(event, params))
      end
    end)
  end

  defp explicit_keys(invalidators, reactors) do
    invalidator_keys(invalidators) ++ reactor_keys(reactors)
  end

  defp invalidator_keys(invalidators) do
    Enum.map(invalidators, fn {notification, on, as} ->
      quote do
        Upkeep.InvalidationSurface.field_key(
          unquote(Macro.escape(notification)),
          unquote(on),
          unquote(as),
          params
        )
      end
    end)
  end

  defp reactor_keys(reactors) do
    Enum.map(reactors, fn {notification, _fun} ->
      quote do
        Upkeep.InvalidationSurface.notification_key(unquote(Macro.escape(notification)))
      end
    end)
  end

  defp explicit_match_body([]), do: false
  defp explicit_match_body(checks), do: Enum.reduce(checks, &{:or, [], [&1, &2]})

  defp reacts_to_body(explicit_checks, defines_query?, query_adapter) do
    query_check = query_check(defines_query?, query_adapter)

    case explicit_checks do
      [] -> query_check
      checks -> Enum.reduce([query_check | checks], &{:or, [], [&1, &2]})
    end
  end

  defp query_check(true, query_adapter) when not is_nil(query_adapter) do
    quote do
      unquote(query_adapter).query_reacts_to?(__MODULE__, event, params, nil)
    end
  end

  defp query_check(_defines_query?, _query_adapter), do: false

  defp load_definition(%{defines_load_1?: true}), do: []
  defp load_definition(%{defines_load_2?: true}), do: []

  defp load_definition(%{defines_query_1?: true, query_adapter: query_adapter})
       when not is_nil(query_adapter) do
    quote do
      def load(params) do
        params
        |> __MODULE__.query()
        |> unquote(query_adapter).read()
      end
    end
  end

  defp load_definition(%{defines_query_2?: true, query_adapter: query_adapter})
       when not is_nil(query_adapter) do
    quote do
      def load(params, context) do
        params
        |> __MODULE__.query(context)
        |> unquote(query_adapter).read()
      end
    end
  end

  defp load_definition(_context) do
    quote do
      def load(_params) do
        raise ArgumentError,
              "#{inspect(__MODULE__)} must define load/1, load/2, query/1, or query/2 to be used as an Upkeep source"
      end

      def load(_params, _context) do
        raise ArgumentError,
              "#{inspect(__MODULE__)} must define load/1, load/2, query/1, or query/2 to be used as an Upkeep source"
      end
    end
  end

  defp explicit_surface([]) do
    quote do
      Upkeep.InvalidationSurface.empty()
    end
  end

  defp explicit_surface(keys) do
    quote do
      Upkeep.InvalidationSurface.manual(
        [unquote_splicing(keys)],
        {__MODULE__, :__upkeep_explicit_surface_matches__?, [params]}
      )
    end
  end

  defp query_surface(true, query_adapter) when not is_nil(query_adapter) do
    quote do
      unquote(query_adapter).query_surface(__MODULE__, params, context)
    end
  end

  defp query_surface(_defines_query?, _query_adapter) do
    quote do
      Upkeep.InvalidationSurface.empty()
    end
  end

  defp sharing_partition_definition(true, _partition_fields), do: []

  defp sharing_partition_definition(false, []) do
    quote do
      def __upkeep_sharing_partition__(params) do
        params
      end
    end
  end

  defp sharing_partition_definition(false, partition_fields) do
    quote do
      def __upkeep_sharing_partition__(params) do
        Map.take(params, unquote(Macro.escape(partition_fields)))
      end
    end
  end

  defp verify_definition(query_adapter) when not is_nil(query_adapter) do
    quote do
      def __upkeep_verify__!(params, opts) do
        unquote(query_adapter).verify_source!(__MODULE__, params, opts)
      end
    end
  end

  defp verify_definition(_query_adapter) do
    quote do
      def __upkeep_verify__!(_params, _opts), do: :ok
    end
  end

  defp build_invalidated_by(notification, opts) do
    on = Keyword.fetch!(opts, :on) |> List.wrap()
    as = Keyword.get(opts, :as, on) |> List.wrap()

    unless length(on) == length(as) do
      raise ArgumentError, "`:on` and `:as` must name the same number of fields"
    end

    quote bind_quoted: [notification: Macro.escape(notification), on: on, as: as] do
      @upkeep_invalidators {notification, on, as}
    end
  end

  defp normalize_notification(name, _caller) when is_atom(name), do: %{name: name, schema: :_}
  defp normalize_notification(event, caller), do: %{event: Macro.expand(event, caller)}

  defp normalize_notification(schema, action, caller) when is_atom(action) do
    %{name: action, schema: Macro.expand(schema, caller)}
  end
end
