defmodule Upkeep.Source do
  @moduledoc """
  Source authoring DSL for Upkeep-style reactive reads.

  A source is a named query plus the domain facts that can invalidate it. The
  common path is equality over event/source fields via `invalidated_by/2`; custom
  predicates stay available through `reacts_to/2`.
  """

  defmacro __using__(_opts) do
    quote do
      import Upkeep.Source,
        only: [query: 1, invalidated_by: 2, reacts_to: 2]

      Module.register_attribute(__MODULE__, :upkeep_invalidators, accumulate: true)
      Module.register_attribute(__MODULE__, :upkeep_reactors, accumulate: true)
      @before_compile Upkeep.Source
    end
  end

  defmacro query(fun) do
    quote do
      def load(params), do: unquote(fun).(params)
    end
  end

  defmacro invalidated_by(event, opts) do
    event = Macro.expand(event, __CALLER__)
    on = Keyword.fetch!(opts, :on) |> List.wrap()
    as = Keyword.get(opts, :as, on) |> List.wrap()

    unless length(on) == length(as) do
      raise ArgumentError, "`:on` and `:as` must name the same number of fields"
    end

    quote bind_quoted: [event: event, on: on, as: as] do
      @upkeep_invalidators {event, on, as}
    end
  end

  defmacro reacts_to(event, fun) do
    event = Macro.expand(event, __CALLER__)

    quote bind_quoted: [event: event, fun: Macro.escape(fun)] do
      @upkeep_reactors {event, fun}
    end
  end

  defmacro __before_compile__(env) do
    invalidators = Module.get_attribute(env.module, :upkeep_invalidators)
    reactors = Module.get_attribute(env.module, :upkeep_reactors)

    invalidator_checks =
      Enum.map(invalidators, fn {event, on, as} ->
        quote do
          match?(%unquote(event){}, event) and
            Upkeep.Source.equal_fields?(event, params, unquote(on), unquote(as))
        end
      end)

    reactor_checks =
      Enum.map(reactors, fn {event, fun} ->
        quote do
          match?(%unquote(event){}, event) and unquote(fun).(event, params)
        end
      end)

    interest_keys =
      Enum.map(invalidators, fn {event, on, as} ->
        quote do
          Upkeep.Source.interest_key(unquote(event), unquote(on), unquote(as), params)
        end
      end) ++
        Enum.map(reactors, fn {event, _fun} ->
          quote do
            Upkeep.Source.event_key(unquote(event))
          end
        end)

    reacts_to_body =
      case invalidator_checks ++ reactor_checks do
        [] -> false
        checks -> Enum.reduce(checks, &{:or, [], [&1, &2]})
      end

    quote do
      def reacts_to?(event, params), do: unquote(reacts_to_body)

      def __upkeep_interest_keys__(params) do
        [unquote_splicing(interest_keys)]
      end
    end
  end

  def equal_fields?(event, params, event_fields, source_fields) do
    event = Map.from_struct(event)

    Enum.zip(event_fields, source_fields)
    |> Enum.all?(fn {event_field, source_field} ->
      Map.fetch!(event, event_field) == Map.fetch!(params, source_field)
    end)
  end

  def interest_key(event, event_fields, source_fields, params) do
    values =
      Enum.zip(event_fields, source_fields)
      |> Enum.map(fn {event_field, source_field} ->
        {event_field, Map.fetch!(params, source_field)}
      end)
      |> Enum.sort()

    {:upkeep_event, event, values}
  end

  def event_key(event), do: {:upkeep_event, event}

  def source_id(source, params) when is_atom(source) and is_map(params), do: {source, params}

  def group_key(term) do
    encoded =
      term
      |> :erlang.term_to_binary()
      |> Base.url_encode64(padding: false)

    "upkeep/source-interest/" <> encoded
  end

  def event_keys(event) do
    event_module = event.__struct__
    fields = Map.from_struct(event)

    field_keys =
      fields
      |> Map.to_list()
      |> non_empty_subsets()
      |> Enum.map(fn values -> {:upkeep_event, event_module, values} end)

    [event_key(event_module) | field_keys]
  end

  defp non_empty_subsets(fields) do
    Enum.reduce(fields, [], fn field, subsets ->
      [[field] | Enum.map(subsets, &[field | &1])] ++ subsets
    end)
    |> Enum.map(&Enum.sort/1)
    |> Enum.uniq()
  end
end
