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
    invalidators = Module.get_attribute(env.module, :upkeep_invalidators)
    reactors = Module.get_attribute(env.module, :upkeep_reactors)
    repo = Module.get_attribute(env.module, :upkeep_repo)
    retry = Module.get_attribute(env.module, :upkeep_retry)
    query_adapter = Module.get_attribute(env.module, :upkeep_query_adapter)
    defines_load? = Module.defines?(env.module, {:load, 1})
    defines_query? = Module.defines?(env.module, {:query, 1})
    defines_sharing_partition? = Module.defines?(env.module, {:__upkeep_sharing_partition__, 1})

    if defines_query? and is_nil(query_adapter) do
      raise ArgumentError,
            "#{inspect(env.module)} defines query/1 but uses Upkeep.Source. " <>
              "Use load/1 for generic sources or use Upkeep.Ecto.Source for Ecto-backed query sources."
    end

    partition_fields =
      invalidators |> Enum.flat_map(fn {_notification, _on, as} -> as end) |> Enum.uniq()

    invalidator_checks =
      Enum.map(invalidators, fn {notification, on, as} ->
        quote do
          Upkeep.Source.Keys.matches?(event, unquote(Macro.escape(notification))) and
            Upkeep.Source.Keys.equal_fields?(event, params, unquote(on), unquote(as))
        end
      end)

    reactor_checks =
      Enum.map(reactors, fn {notification, fun} ->
        quote do
          Upkeep.Source.Keys.matches?(event, unquote(Macro.escape(notification))) and
            (Upkeep.Change.broad_update?(event) or Upkeep.Change.partial_update?(event) or
               unquote(fun).(event, params))
        end
      end)

    interest_keys =
      Enum.map(invalidators, fn {notification, on, as} ->
        quote do
          Upkeep.Source.Keys.interest_key(
            unquote(Macro.escape(notification)),
            unquote(on),
            unquote(as),
            params
          )
        end
      end) ++
        Enum.map(reactors, fn {notification, _fun} ->
          quote do
            Upkeep.Source.Keys.notification_key(unquote(Macro.escape(notification)))
          end
        end)

    reacts_to_body =
      case invalidator_checks ++ reactor_checks do
        [] ->
          if defines_query? and query_adapter do
            quote do
              unquote(query_adapter).query_reacts_to?(__MODULE__, event, params)
            end
          else
            false
          end

        checks ->
          query_check =
            if defines_query? and query_adapter do
              quote do
                unquote(query_adapter).query_reacts_to?(__MODULE__, event, params)
              end
            else
              false
            end

          Enum.reduce([query_check | checks], &{:or, [], [&1, &2]})
      end

    load_definition =
      cond do
        defines_load? ->
          []

        defines_query? and query_adapter ->
          quote do
            def load(params) do
              params
              |> __MODULE__.query()
              |> unquote(query_adapter).read()
            end
          end

        true ->
          quote do
            def load(_params) do
              raise ArgumentError,
                    "#{inspect(__MODULE__)} must define load/1 or query/1 to be used as an Upkeep source"
            end
          end
      end

    query_interest_keys =
      if defines_query? and query_adapter do
        quote do
          unquote(query_adapter).query_interest_keys(__MODULE__, params)
        end
      else
        []
      end

    sharing_partition_definition =
      cond do
        defines_sharing_partition? ->
          []

        partition_fields == [] ->
          quote do
            def __upkeep_sharing_partition__(params) do
              params
            end
          end

        true ->
          quote do
            def __upkeep_sharing_partition__(params) do
              Map.take(params, unquote(Macro.escape(partition_fields)))
            end
          end
      end

    verify_definition =
      if query_adapter do
        quote do
          def __upkeep_verify__!(params, opts) do
            unquote(query_adapter).verify_source!(__MODULE__, params, opts)
          end
        end
      else
        quote do
          def __upkeep_verify__!(_params, _opts), do: :ok
        end
      end

    quote do
      unquote(load_definition)
      unquote(sharing_partition_definition)
      unquote(verify_definition)

      def __upkeep_repo__, do: unquote(repo)
      def __upkeep_repo_explicit__?, do: unquote(not is_nil(repo))
      def __upkeep_query_source__?, do: unquote(defines_query?)
      def __upkeep_retry__, do: unquote(Macro.escape(retry))

      def reacts_to?(event, params), do: unquote(reacts_to_body)

      def __upkeep_interest_keys__(params) do
        [unquote_splicing(interest_keys)] ++ unquote(query_interest_keys)
      end

      def __upkeep_explicit_interest_keys__(params) do
        [unquote_splicing(interest_keys)]
      end
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
  defp normalize_notification(event, caller), do: %{legacy: Macro.expand(event, caller)}

  defp normalize_notification(schema, action, caller) when is_atom(action) do
    %{name: action, schema: Macro.expand(schema, caller)}
  end
end
