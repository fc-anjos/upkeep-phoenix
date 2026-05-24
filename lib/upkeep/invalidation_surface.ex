defmodule Upkeep.InvalidationSurface do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [
      Index,
      Index.ETS
    ],
    deps: [
      Ecto.Type,
      Upkeep.Change
    ],
    type: :strict

  alias Upkeep.Change

  defstruct keys: [],
            index_keys: [],
            matcher: nil

  @type key :: term()
  @type notification :: %{required(:name) => atom(), optional(:schema) => module() | :_}
  @type event_notification :: %{required(:event) => module()}
  @type matcher :: nil | (struct() -> boolean()) | {module(), atom(), [term()]}
  @type t :: %__MODULE__{
          keys: [key()],
          index_keys: [key()],
          matcher: matcher()
        }

  def empty, do: %__MODULE__{}

  @spec manual([key()], matcher()) :: t()
  def manual(keys, matcher) when is_list(keys) do
    unless is_function(matcher, 1) or valid_matcher?(matcher) do
      raise ArgumentError, "invalid invalidation surface matcher"
    end

    %__MODULE__{
      keys: Enum.uniq(keys),
      index_keys: build_index_keys(keys),
      matcher: matcher
    }
  end

  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = left, %__MODULE__{} = right) do
    %__MODULE__{
      keys: Enum.uniq(left.keys ++ right.keys),
      index_keys: Enum.uniq(left.index_keys ++ right.index_keys),
      matcher: merge_matchers(left.matcher, right.matcher)
    }
  end

  @spec merge_all([t()]) :: t()
  def merge_all(surfaces) when is_list(surfaces) do
    Enum.reduce(surfaces, empty(), &merge/2)
  end

  @spec keys(t()) :: [key()]
  def keys(%__MODULE__{keys: keys}), do: keys

  @spec index_keys(t()) :: [key()]
  def index_keys(%__MODULE__{index_keys: keys}), do: keys

  @spec candidate_keys(struct()) :: [key()]
  def candidate_keys(%Change{} = change) do
    [
      notification_key(%{name: change.name, schema: change.schema}),
      notification_key(%{name: change.name, schema: :_})
    ]
    |> Enum.uniq()
  end

  def candidate_keys(event) when is_struct(event) do
    [notification_key(%{event: event.__struct__})]
  end

  @spec index_query(struct()) ::
          {:broad, [key()]}
          | {:partial, [key()], MapSet.t(atom()), [map()]}
          | {:exact, [key()], [map()]}
  def index_query(%Change{} = change) do
    notifications = candidate_keys(change)

    cond do
      Change.broad_update?(change) ->
        {:broad, notifications}

      Change.partial_update?(change) ->
        {:partial, notifications, Change.changed_fields(change), Change.field_sets(change)}

      true ->
        {:exact, notifications, Change.field_sets(change)}
    end
  end

  def index_query(event) when is_struct(event) do
    {:exact, candidate_keys(event), [Map.from_struct(event)]}
  end

  @spec event_metadata(struct()) :: map()
  def event_metadata(%Change{} = change) do
    %{
      kind: :change,
      name: change.name,
      action: change.action,
      schema: change.schema
    }
  end

  def event_metadata(event) when is_struct(event) do
    %{kind: :event, event_module: event.__struct__}
  end

  @spec matches?(t(), struct()) :: boolean()
  def matches?(%__MODULE__{} = surface, event) when is_struct(event) do
    manual_match?(surface.matcher, event)
  end

  @spec matches_notification?(struct(), notification() | event_notification()) :: boolean()
  def matches_notification?(%Change{} = change, %{name: name, schema: schema}) do
    change.name == name and schema_matches?(change.schema, schema)
  end

  def matches_notification?(event, %{event: event_module}) when is_struct(event) do
    event.__struct__ == event_module
  end

  def matches_notification?(_event, _notification), do: false

  @spec equal_fields?(struct(), map(), [atom()], [atom()]) :: boolean()
  def equal_fields?(%Change{} = change, params, event_fields, source_fields) do
    cond do
      Change.partial_update?(change) ->
        change
        |> Change.field_sets()
        |> Enum.any?(fn fields ->
          partial_equal_field_set?(change, fields, params, event_fields, source_fields)
        end)

      Change.broad_update?(change) ->
        true

      true ->
        change
        |> Change.field_sets()
        |> Enum.any?(fn fields ->
          equal_field_set?(fields, params, event_fields, source_fields)
        end)
    end
  end

  def equal_fields?(event, params, event_fields, source_fields) when is_struct(event) do
    event
    |> Map.from_struct()
    |> equal_field_set?(params, event_fields, source_fields)
  end

  @spec field_key(notification() | event_notification(), [atom()], [atom()], map()) :: key()
  def field_key(notification, event_fields, source_fields, params) do
    values =
      Enum.zip(event_fields, source_fields)
      |> Enum.map(fn {event_field, source_field} ->
        {event_field, Map.fetch!(params, source_field)}
      end)
      |> Enum.sort()

    notification_key(notification, values)
  end

  @doc """
  Normalizes a value to a single canonical Elixir term using the Ecto type of
  `field` on `schema`.

  Index-key matching compares the query param value captured when a surface is
  built against the record field value carried by a committed change. Those two
  sides can have skewed representations: a string param against an integer
  column (`"7"` vs `7`), or an `Ecto.Enum` queried with an atom (`:open`) against
  a change carrying the dumped value (`"open"`, as bulk `insert_all`/`update_all`
  paths produce). Casting both sides through the field's Ecto type collapses them
  to the same loaded term so equality matching is correct.

  Falls back to the raw value when there is no schema (schemaless `@table`/
  `field/2` sources), the field/type is unavailable, or the cast fails. It never
  raises.
  """
  @spec canonical_value(module() | binary() | nil, atom(), term()) :: term()
  def canonical_value(schema, field, value) do
    case field_type(schema, field) do
      {:ok, type} -> cast_value(type, value)
      :error -> value
    end
  end

  defp field_type(schema, field) when is_atom(schema) and not is_nil(schema) and is_atom(field) do
    if function_exported?(schema, :__schema__, 2) do
      case schema.__schema__(:type, field) do
        nil -> :error
        type -> {:ok, type}
      end
    else
      :error
    end
  rescue
    _exception -> :error
  end

  defp field_type(_schema, _field), do: :error

  defp cast_value(type, value) do
    case Ecto.Type.cast(type, value) do
      {:ok, cast} -> cast
      _other -> value
    end
  rescue
    _exception -> value
  end

  def notification_key(%{event: event}), do: {:upkeep_event, event}
  def notification_key(%{name: name, schema: schema}), do: {:upkeep_change, name, schema}
  def notification_key(%{event: event}, values), do: {:upkeep_event, event, values}

  def notification_key(%{name: name, schema: schema}, values),
    do: {:upkeep_change, name, schema, values}

  def event_keys(%Upkeep.Change{} = change) do
    [
      notification_key(%{name: change.name, schema: change.schema}),
      notification_key(%{name: change.name, schema: :_})
    ]
  end

  def event_keys(event) when is_struct(event) do
    [notification_key(%{event: event.__struct__})]
  end

  defp build_index_keys(keys) do
    keys
    |> Enum.flat_map(&coarse_key/1)
    |> Enum.uniq()
  end

  defp coarse_key({:upkeep_change, name, schema}), do: [{:upkeep_change, name, schema}]
  defp coarse_key({:upkeep_change, name, schema, _values}), do: [{:upkeep_change, name, schema}]
  defp coarse_key({:upkeep_event, event}), do: [{:upkeep_event, event}]
  defp coarse_key({:upkeep_event, event, _values}), do: [{:upkeep_event, event}]
  defp coarse_key({action, schema}) when is_atom(action), do: [{:upkeep_change, action, schema}]
  defp coarse_key(_key), do: []

  defp equal_field_set?(fields, params, event_fields, source_fields) do
    Enum.zip(event_fields, source_fields)
    |> Enum.all?(fn {event_field, source_field} ->
      Map.fetch!(fields, event_field) == Map.fetch!(params, source_field)
    end)
  end

  defp partial_equal_field_set?(change, fields, params, event_fields, source_fields) do
    Enum.zip(event_fields, source_fields)
    |> Enum.all?(fn {event_field, source_field} ->
      Upkeep.Change.field_change(change, event_field) == :changed or
        Map.fetch!(fields, event_field) == Map.fetch!(params, source_field)
    end)
  end

  defp schema_matches?(_actual, :_), do: true
  defp schema_matches?(schema, schema), do: true
  defp schema_matches?(_actual, _expected), do: false

  defp merge_matchers(nil, matcher), do: matcher
  defp merge_matchers(matcher, nil), do: matcher

  defp merge_matchers(left, right) do
    {__MODULE__, :any_match?, [[left, right]]}
  end

  def any_match?(matchers, event) when is_list(matchers) do
    Enum.any?(matchers, &manual_match?(&1, event))
  end

  defp manual_match?(nil, _event), do: false
  defp manual_match?(matcher, event) when is_function(matcher, 1), do: matcher.(event)

  defp manual_match?({module, function, args}, event)
       when is_atom(module) and is_atom(function) and is_list(args) do
    apply(module, function, args ++ [event])
  end

  defp valid_matcher?({module, function, args})
       when is_atom(module) and is_atom(function) and is_list(args),
       do: true

  defp valid_matcher?(_matcher), do: false
end
