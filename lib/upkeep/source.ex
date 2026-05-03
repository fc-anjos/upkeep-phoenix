defmodule Upkeep.Source do
  @moduledoc """
  Source authoring helpers for Upkeep-style reactive reads.

  A source is a module that can load a live read. Plain `load/1` functions are
  supported directly; Ecto-backed sources can expose a Phoenix-style `query/1`
  and let Upkeep infer common invalidation keys from the returned query.
  """

  defmacro __using__(opts) do
    repo =
      opts
      |> Keyword.get(:repo)
      |> Macro.expand(__CALLER__)

    quote bind_quoted: [repo: repo] do
      import Upkeep.Source,
        only: [query: 1, invalidated_by: 2, invalidated_by: 3, reacts_to: 2, reacts_to: 3]

      @upkeep_repo repo
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

  defmacro invalidated_by(notification, opts) do
    build_invalidated_by(normalize_notification(notification, __CALLER__), opts)
  end

  defmacro invalidated_by(schema, action, opts) do
    notification = normalize_notification(schema, action, __CALLER__)
    build_invalidated_by(notification, opts)
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
    defines_load? = Module.defines?(env.module, {:load, 1})
    defines_query? = Module.defines?(env.module, {:query, 1})

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
            unquote(fun).(event, params)
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
              |> Upkeep.Source.load_from_query(unquote(repo))
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

    quote do
      unquote(load_definition)

      def reacts_to?(event, params), do: unquote(reacts_to_body)

      def __upkeep_interest_keys__(params) do
        [unquote_splicing(interest_keys)] ++ unquote(query_interest_keys)
      end
    end
  end

  def load_from_query(%Ecto.Query{} = _query, nil) do
    raise ArgumentError,
          "source returned an Ecto.Query but no repo was configured; " <>
            "use `use Upkeep.Source, repo: MyApp.Repo` or define load/1 explicitly"
  end

  def load_from_query(%Ecto.Query{} = query, repo) when is_atom(repo), do: repo.all(query)
  def load_from_query(value, _repo), do: value

  def query_interest_keys(source, params) when is_atom(source) do
    source
    |> source_query(params)
    |> Upkeep.Ecto.QueryDeps.interest_keys()
  end

  def query_reacts_to?(source, event, params) when is_atom(source) and is_struct(event) do
    deps =
      source
      |> source_query(params)
      |> Upkeep.Ecto.QueryDeps.from_query()

    Upkeep.Ecto.QueryDeps.matches_change?(deps, event)
  end

  defp source_query(source, params) do
    if function_exported?(source, :query, 1), do: source.query(params), else: nil
  end

  def matches?(%Upkeep.Change{} = change, %{name: name, schema: schema}) do
    change.name == name and schema_matches?(change.schema, schema)
  end

  def matches?(event, %{legacy: event_module}) when is_struct(event) do
    match?(%{__struct__: ^event_module}, event)
  end

  def equal_fields?(%Upkeep.Change{} = change, params, event_fields, source_fields) do
    change
    |> Upkeep.Change.field_sets()
    |> Enum.any?(fn fields ->
      equal_field_set?(fields, params, event_fields, source_fields)
    end)
  end

  def equal_fields?(event, params, event_fields, source_fields) when is_struct(event) do
    event
    |> Map.from_struct()
    |> equal_field_set?(params, event_fields, source_fields)
  end

  defp equal_field_set?(fields, params, event_fields, source_fields) do
    Enum.zip(event_fields, source_fields)
    |> Enum.all?(fn {event_field, source_field} ->
      Map.fetch!(fields, event_field) == Map.fetch!(params, source_field)
    end)
  end

  def interest_key(notification, event_fields, source_fields, params) do
    values =
      Enum.zip(event_fields, source_fields)
      |> Enum.map(fn {event_field, source_field} ->
        {event_field, Map.fetch!(params, source_field)}
      end)
      |> Enum.sort()

    notification_key(notification, values)
  end

  def notification_key(%{legacy: event}), do: {:upkeep_event, event}
  def notification_key(%{name: name, schema: schema}), do: {:upkeep_change, name, schema}

  def notification_key(%{legacy: event}, values), do: {:upkeep_event, event, values}

  def notification_key(%{name: name, schema: schema}, values),
    do: {:upkeep_change, name, schema, values}

  def source_id(source, params) when is_atom(source) and is_map(params), do: {source, params}

  def group_key(term) do
    encoded =
      term
      |> :erlang.term_to_binary()
      |> Base.url_encode64(padding: false)

    "upkeep/source-interest/" <> encoded
  end

  def event_keys(%Upkeep.Change{} = change) do
    field_keys =
      change
      |> Upkeep.Change.field_sets()
      |> Enum.flat_map(fn fields ->
        fields
        |> Map.to_list()
        |> non_empty_subsets()
        |> Enum.flat_map(fn values ->
          [
            notification_key(%{name: change.name, schema: change.schema}, values),
            notification_key(%{name: change.name, schema: :_}, values)
          ]
        end)
      end)

    [
      notification_key(%{name: change.name, schema: change.schema}),
      notification_key(%{name: change.name, schema: :_})
      | field_keys
    ]
    |> Enum.uniq()
  end

  def event_keys(event) when is_struct(event) do
    event_module = event.__struct__
    fields = Map.from_struct(event)

    field_keys =
      fields
      |> Map.to_list()
      |> non_empty_subsets()
      |> Enum.map(fn values -> {:upkeep_event, event_module, values} end)

    [notification_key(%{legacy: event_module}) | field_keys]
  end

  defp normalize_notification(name, _caller) when is_atom(name), do: %{name: name, schema: :_}
  defp normalize_notification(event, caller), do: %{legacy: Macro.expand(event, caller)}

  defp normalize_notification(schema, action, caller) when is_atom(action) do
    %{name: action, schema: Macro.expand(schema, caller)}
  end

  defp schema_matches?(_actual, :_), do: true
  defp schema_matches?(schema, schema), do: true
  defp schema_matches?(_actual, _expected), do: false

  defp non_empty_subsets(fields) do
    Enum.reduce(fields, [], fn field, subsets ->
      [[field] | Enum.map(subsets, &[field | &1])] ++ subsets
    end)
    |> Enum.map(&Enum.sort/1)
    |> Enum.uniq()
  end
end
