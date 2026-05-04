defmodule Upkeep.Source do
  @moduledoc """
  Source authoring helpers for Upkeep-style reactive reads.

  A source is a module that can load a live read. Ecto-backed sources should
  perform database reads through `Upkeep.read/1`; those reads are tracked as
  the source's reactive surface.
  """

  @context_key {__MODULE__, :read_context}

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

    quote do
      unquote(load_definition)

      def __upkeep_repo__, do: unquote(repo)

      def reacts_to?(event, params), do: unquote(reacts_to_body)

      def __upkeep_interest_keys__(params) do
        [unquote_splicing(interest_keys)] ++ unquote(query_interest_keys)
      end
    end
  end

  def load(source, params) when is_atom(source) do
    repo = source.__upkeep_repo__()

    {value, deps} =
      with_read_context(repo, fn ->
        value = source.load(params)
        {value, tracked_deps()}
      end)

    warn_if_no_invalidation_surface(source, params, deps)
    {value, deps}
  end

  def read(%Ecto.Query{} = query) do
    case Process.get(@context_key) do
      %{repo: repo} ->
        track_query(query)
        repo.all(query)

      _ ->
        raise ArgumentError,
              "Upkeep.read/1 must be called inside a source context. " <>
                "Use it only inside a source's load/1 or query/1 callback. " <>
                "For ad-hoc queries, call Repo.all/1 directly."
    end
  end

  def read(value), do: value

  def deps_interest_keys(deps) do
    deps
    |> Enum.flat_map(&Upkeep.Ecto.QueryDeps.interest_keys/1)
    |> Enum.uniq()
  end

  def deps_react_to?(deps, event) do
    Enum.any?(deps, &Upkeep.Ecto.QueryDeps.matches_change?(&1, event))
  end

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

  defp with_read_context(repo, fun) do
    previous = Process.get(@context_key)
    Process.put(@context_key, %{repo: repo, deps: []})

    try do
      fun.()
    after
      restore_read_context(previous)
    end
  end

  defp restore_read_context(nil), do: Process.delete(@context_key)
  defp restore_read_context(previous), do: Process.put(@context_key, previous)

  defp track_query(query) do
    case Process.get(@context_key) do
      %{deps: deps} = context ->
        deps = [Upkeep.Ecto.QueryDeps.from_query(query) | deps]
        Process.put(@context_key, %{context | deps: deps})

      _ ->
        raise "Upkeep.Source.track_query/1 called outside a source context. " <>
                "This usually means a Task spawned inside load/1 lost the context — " <>
                "explicit propagation is required for concurrent reads."
    end
  end

  @warn_dedup_key {__MODULE__, :no_invalidation_warned}

  defp warn_if_no_invalidation_surface(source, params, deps) do
    static_keys =
      if function_exported?(source, :__upkeep_interest_keys__, 1),
        do: source.__upkeep_interest_keys__(params),
        else: []

    if static_keys == [] and deps == [] do
      shape = {source, params}
      seen = :persistent_term.get(@warn_dedup_key, MapSet.new())

      unless MapSet.member?(seen, shape) do
        :persistent_term.put(@warn_dedup_key, MapSet.put(seen, shape))

        require Logger

        Logger.warning(
          "Upkeep source #{inspect(source)} with params #{inspect(params)} produced " <>
            "no invalidation keys. It will not react to any event. Add an " <>
            "invalidated_by/reacts_to declaration, or call Upkeep.read inside load/1."
        )
      end
    end

    :ok
  end

  defp tracked_deps do
    case Process.get(@context_key) do
      %{deps: deps} -> Enum.reverse(deps)
      _context -> []
    end
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
