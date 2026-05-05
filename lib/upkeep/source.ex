defmodule Upkeep.Source do
  @moduledoc """
  Source authoring helpers for Upkeep-style reactive reads.

  A source is a module that can load a live read. Ecto-backed sources should
  perform database reads through `Upkeep.read/1`; those reads are tracked as
  the source's reactive surface.
  """

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

  defdelegate read(query_or_value), to: Upkeep.Source.Runtime
  defdelegate coverage(source, params), to: Upkeep.Source.Runtime
  defdelegate coverage(source, params, deps), to: Upkeep.Source.Runtime

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
