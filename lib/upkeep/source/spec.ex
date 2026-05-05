defmodule Upkeep.Source.Spec do
  @moduledoc false

  defmacro __using__(opts) do
    repo =
      opts
      |> Keyword.get(:repo)
      |> Macro.expand(__CALLER__)

    retry = Keyword.get(opts, :retry, :default)

    quote bind_quoted: [repo: repo, retry: retry] do
      import Upkeep.Source,
        only: [query: 1, invalidated_by: 2, invalidated_by: 3, reacts_to: 2, reacts_to: 3]

      @upkeep_repo repo
      @upkeep_retry retry
      Module.register_attribute(__MODULE__, :upkeep_invalidators, accumulate: true)
      Module.register_attribute(__MODULE__, :upkeep_reactors, accumulate: true)
      @before_compile Upkeep.Source.Spec
    end
  end

  defmacro query(fun) do
    quote do
      def load(params), do: unquote(fun).(params)
    end
  end

  defmacro invalidated_by(notification, opts) do
    build_invalidated_by(normalize_notification(notification, __CALLER__), opts)
  end

  defmacro invalidated_by(schema, action, opts) do
    notification = normalize_notification(schema, action, __CALLER__)
    build_invalidated_by(notification, opts)
  end

  defmacro reacts_to(notification, fun) do
    notification = normalize_notification(notification, __CALLER__)

    quote bind_quoted: [notification: Macro.escape(notification), fun: Macro.escape(fun)] do
      @upkeep_reactors {notification, fun}
    end
  end

  defmacro reacts_to(schema, action, fun) do
    notification = normalize_notification(schema, action, __CALLER__)

    quote bind_quoted: [notification: Macro.escape(notification), fun: Macro.escape(fun)] do
      @upkeep_reactors {notification, fun}
    end
  end

  defmacro __before_compile__(env) do
    invalidators = Module.get_attribute(env.module, :upkeep_invalidators)
    reactors = Module.get_attribute(env.module, :upkeep_reactors)
    repo = Module.get_attribute(env.module, :upkeep_repo)
    retry = Module.get_attribute(env.module, :upkeep_retry)
    defines_load? = Module.defines?(env.module, {:load, 1})
    defines_query? = Module.defines?(env.module, {:query, 1})
    defines_sharing_partition? = Module.defines?(env.module, {:__upkeep_sharing_partition__, 1})

    partition_fields =
      invalidators |> Enum.flat_map(fn {_notification, _on, as} -> as end) |> Enum.uniq()

    invalidator_checks =
      Enum.map(invalidators, fn {notification, on, as} ->
        quote do
          Upkeep.Source.matches?(event, unquote(Macro.escape(notification))) and
            Upkeep.Source.equal_fields?(event, params, unquote(on), unquote(as))
        end
      end)

    reactor_checks =
      Enum.map(reactors, fn {notification, fun} ->
        quote do
          Upkeep.Source.matches?(event, unquote(Macro.escape(notification))) and
            (Upkeep.Change.broad_update?(event) or unquote(fun).(event, params))
        end
      end)

    interest_keys =
      Enum.map(invalidators, fn {notification, on, as} ->
        quote do
          Upkeep.Source.interest_key(
            unquote(Macro.escape(notification)),
            unquote(on),
            unquote(as),
            params
          )
        end
      end) ++
        Enum.map(reactors, fn {notification, _fun} ->
          quote do
            Upkeep.Source.notification_key(unquote(Macro.escape(notification)))
          end
        end)

    reacts_to_body =
      case invalidator_checks ++ reactor_checks do
        [] ->
          if defines_query? do
            quote do
              Upkeep.Source.query_reacts_to?(__MODULE__, event, params)
            end
          else
            false
          end

        checks ->
          query_check =
            if defines_query? do
              quote do
                Upkeep.Source.query_reacts_to?(__MODULE__, event, params)
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

        defines_query? ->
          quote do
            def load(params) do
              params
              |> __MODULE__.query()
              |> Upkeep.read()
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
      if defines_query? do
        quote do
          Upkeep.Source.query_interest_keys(__MODULE__, params)
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

    quote do
      unquote(load_definition)
      unquote(sharing_partition_definition)

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
