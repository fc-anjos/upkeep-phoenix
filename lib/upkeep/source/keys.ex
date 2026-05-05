defmodule Upkeep.Source.Keys do
  @moduledoc false

  def matches?(%Upkeep.Change{} = change, %{name: name, schema: schema}) do
    change.name == name and schema_matches?(change.schema, schema)
  end

  def matches?(event, %{legacy: event_module}) when is_struct(event) do
    match?(%{__struct__: ^event_module}, event)
  end

  def equal_fields?(%Upkeep.Change{} = change, params, event_fields, source_fields) do
    if Upkeep.Change.broad_update?(change) do
      true
    else
      change
      |> Upkeep.Change.field_sets()
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

  def event_keys(%Upkeep.Change{} = change) do
    broad_keys = [
      notification_key(%{name: change.name, schema: change.schema}),
      notification_key(%{name: change.name, schema: :_})
    ]

    field_keys =
      if Upkeep.Change.broad_update?(change) do
        []
      else
        field_event_keys(change)
      end

    (broad_keys ++ field_keys)
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

  defp equal_field_set?(fields, params, event_fields, source_fields) do
    Enum.zip(event_fields, source_fields)
    |> Enum.all?(fn {event_field, source_field} ->
      Map.fetch!(fields, event_field) == Map.fetch!(params, source_field)
    end)
  end

  defp field_event_keys(%Upkeep.Change{} = change) do
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
