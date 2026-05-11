defmodule Upkeep.InvalidationSurface.Index do
  @moduledoc false

  alias Upkeep.InvalidationSurface

  defstruct exact: %{},
            notifications: %{},
            field_sets: %{},
            field_set_index: %{},
            unindexed: MapSet.new()

  def new, do: %__MODULE__{}

  def rebuild(entries) do
    Enum.reduce(entries, new(), fn {id, surface}, index ->
      put(index, id, surface)
    end)
  end

  def put(%__MODULE__{} = index, id, %InvalidationSurface{} = surface) do
    case index_entries(surface) do
      {:indexed, keys} ->
        put_indexed_keys(index, id, keys)

      {:mixed, keys} ->
        index
        |> put_indexed_keys(id, keys)
        |> put_unindexed(id)

      :unindexed ->
        put_unindexed(index, id)
    end
  end

  def put(%__MODULE__{} = index, id, _surface) do
    put_unindexed(index, id)
  end

  def delete(%__MODULE__{} = index, id, %InvalidationSurface{} = surface) do
    case index_entries(surface) do
      {:indexed, keys} ->
        delete_indexed_keys(index, id, keys)

      {:mixed, keys} ->
        index
        |> delete_indexed_keys(id, keys)
        |> delete_unindexed(id)

      :unindexed ->
        delete_unindexed(index, id)
    end
  end

  def delete(%__MODULE__{} = index, id, _surface) do
    delete_unindexed(index, id)
  end

  def candidates(%__MODULE__{} = index, event) when is_struct(event) do
    event
    |> lookup_terms(&registered_field_names(&1, index))
    |> elem(1)
    |> Enum.reduce(MapSet.new(), fn term, ids ->
      index
      |> lookup_ids(term)
      |> MapSet.union(ids)
    end)
  end

  def surface_terms(%InvalidationSurface{} = surface) do
    case index_entries(surface) do
      {:indexed, keys} -> indexed_terms(keys)
      {:mixed, keys} -> [:unindexed | indexed_terms(keys)]
      :unindexed -> [:unindexed]
    end
  end

  def surface_terms(_surface), do: [:unindexed]

  def lookup_terms(event, field_names_fun)
      when is_struct(event) and is_function(field_names_fun, 1) do
    terms =
      event
      |> InvalidationSurface.index_query()
      |> query_terms(field_names_fun)
      |> Enum.uniq()

    {terms, Enum.uniq([:unindexed | terms])}
  end

  defp put_indexed_keys(index, id, keys) do
    Enum.reduce(keys, index, &put_indexed_key(&2, id, &1))
  end

  defp delete_indexed_keys(index, id, keys) do
    Enum.reduce(keys, index, &delete_indexed_key(&2, id, &1))
  end

  defp put_indexed_key(index, id, key) do
    index
    |> put_exact(key, id)
    |> put_notification(key, id)
    |> put_field_set(key, id)
  end

  defp delete_indexed_key(index, id, key) do
    index
    |> delete_exact(key, id)
    |> delete_notification(key, id)
    |> delete_field_set(key, id)
  end

  defp put_exact(index, key, id) do
    %{index | exact: put_index_id(index.exact, key, id)}
  end

  defp delete_exact(index, key, id) do
    {exact, _empty?} = delete_index_id(index.exact, key, id)
    %{index | exact: exact}
  end

  defp put_notification(index, key, id) do
    case notification_key(key) do
      nil ->
        index

      notification ->
        %{index | notifications: put_index_id(index.notifications, notification, id)}
    end
  end

  defp delete_notification(index, key, id) do
    case notification_key(key) do
      nil ->
        index

      notification ->
        {notifications, _empty?} = delete_index_id(index.notifications, notification, id)
        %{index | notifications: notifications}
    end
  end

  defp put_field_set(index, key, id) do
    case field_set_key(key) do
      nil ->
        index

      {notification, field_names} = field_set_key ->
        field_set_index = put_index_id(index.field_set_index, field_set_key, id)

        field_sets =
          Map.update(
            index.field_sets,
            notification,
            MapSet.new([field_names]),
            &MapSet.put(&1, field_names)
          )

        %{index | field_set_index: field_set_index, field_sets: field_sets}
    end
  end

  defp delete_field_set(index, key, id) do
    case field_set_key(key) do
      nil ->
        index

      {notification, field_names} = field_set_key ->
        {field_set_index, empty?} = delete_index_id(index.field_set_index, field_set_key, id)

        field_sets =
          if empty? do
            delete_field_names(index.field_sets, notification, field_names)
          else
            index.field_sets
          end

        %{index | field_set_index: field_set_index, field_sets: field_sets}
    end
  end

  defp put_index_id(map, key, id) do
    Map.update(map, key, MapSet.new([id]), &MapSet.put(&1, id))
  end

  defp delete_index_id(map, key, id) do
    case Map.fetch(map, key) do
      {:ok, ids} ->
        ids = MapSet.delete(ids, id)

        if MapSet.size(ids) == 0 do
          {Map.delete(map, key), true}
        else
          {Map.put(map, key, ids), false}
        end

      :error ->
        {map, false}
    end
  end

  defp delete_field_names(field_sets, notification, field_names) do
    case Map.fetch(field_sets, notification) do
      {:ok, names} ->
        names = MapSet.delete(names, field_names)

        if MapSet.size(names) == 0 do
          Map.delete(field_sets, notification)
        else
          Map.put(field_sets, notification, names)
        end

      :error ->
        field_sets
    end
  end

  defp put_unindexed(index, id) do
    %{index | unindexed: MapSet.put(index.unindexed, id)}
  end

  defp delete_unindexed(index, id) do
    %{index | unindexed: MapSet.delete(index.unindexed, id)}
  end

  defp lookup_ids(index, {:exact, key}) do
    Map.get(index.exact, key, MapSet.new())
  end

  defp lookup_ids(index, {:notification, notification}) do
    Map.get(index.notifications, notification, MapSet.new())
  end

  defp lookup_ids(index, {:field_set, field_set_key}) do
    Map.get(index.field_set_index, field_set_key, MapSet.new())
  end

  defp lookup_ids(index, :unindexed) do
    index.unindexed
  end

  defp query_terms({:broad, notifications}, _field_names_fun) do
    Enum.map(notifications, &{:notification, &1})
  end

  defp query_terms({:partial, notifications, changed_fields, field_maps}, field_names_fun) do
    Enum.flat_map(notifications, fn notification ->
      [
        {:exact, notification}
        | partial_field_terms(notification, changed_fields, field_maps, field_names_fun)
      ]
    end)
  end

  defp query_terms({:exact, notifications, field_maps}, field_names_fun) do
    Enum.flat_map(notifications, fn notification ->
      [
        {:exact, notification}
        | exact_terms_for_registered_fields(notification, field_maps, field_names_fun)
      ]
    end)
  end

  defp partial_field_terms(notification, changed_fields, field_maps, field_names_fun) do
    notification
    |> field_names_fun.()
    |> Enum.flat_map(fn field_names ->
      if changed_field_set?(field_names, changed_fields) do
        [{:field_set, {notification, field_names}}]
      else
        exact_terms_for_field_names(notification, field_names, field_maps)
      end
    end)
  end

  defp exact_terms_for_registered_fields(notification, field_maps, field_names_fun) do
    notification
    |> field_names_fun.()
    |> Enum.flat_map(&exact_terms_for_field_names(notification, &1, field_maps))
  end

  defp exact_terms_for_field_names(notification, field_names, field_maps) do
    Enum.flat_map(field_maps, fn fields ->
      case field_key(notification, field_names, fields) do
        nil -> []
        key -> [{:exact, key}]
      end
    end)
  end

  defp changed_field_set?(field_names, changed_fields) do
    field_names
    |> Tuple.to_list()
    |> Enum.any?(&MapSet.member?(changed_fields, &1))
  end

  defp indexed_terms(keys) do
    keys
    |> Enum.flat_map(fn key ->
      [{:exact, key} | notification_terms(key) ++ field_set_terms(key)]
    end)
    |> Enum.uniq()
  end

  defp notification_terms(key) do
    case notification_key(key) do
      nil -> []
      notification -> [{:notification, notification}]
    end
  end

  defp field_set_terms(key) do
    case field_set_key(key) do
      nil ->
        []

      {notification, field_names} = field_set_key ->
        [{:field_set, field_set_key}, {:field_names, notification, field_names}]
    end
  end

  defp registered_field_names(notification, index) do
    index.field_sets
    |> Map.get(notification, MapSet.new())
    |> MapSet.to_list()
  end

  defp index_entries(%InvalidationSurface{} = surface) do
    keys = InvalidationSurface.keys(surface)
    indexable = Enum.filter(keys, &indexable_key?/1)
    unindexable? = Enum.any?(keys, &(not indexable_key?(&1)))

    cond do
      indexable == [] ->
        :unindexed

      unindexable? ->
        {:mixed, indexable}

      true ->
        {:indexed, indexable}
    end
  end

  defp indexable_key?({:upkeep_change, _name, _schema}), do: true
  defp indexable_key?({:upkeep_change, _name, _schema, values}), do: keyword_values?(values)
  defp indexable_key?({:upkeep_event, _event}), do: true
  defp indexable_key?({:upkeep_event, _event, values}), do: keyword_values?(values)
  defp indexable_key?(_key), do: false

  defp keyword_values?(values) when is_list(values) do
    Enum.all?(values, fn
      {field, _value} when is_atom(field) -> true
      _other -> false
    end)
  end

  defp keyword_values?(_values), do: false

  defp notification_key({:upkeep_change, name, schema}), do: {:upkeep_change, name, schema}

  defp notification_key({:upkeep_change, name, schema, _values}),
    do: {:upkeep_change, name, schema}

  defp notification_key({:upkeep_event, event}), do: {:upkeep_event, event}
  defp notification_key({:upkeep_event, event, _values}), do: {:upkeep_event, event}
  defp notification_key(_key), do: nil

  defp field_set_key({:upkeep_change, name, schema, values}) do
    {{:upkeep_change, name, schema}, field_names(values)}
  end

  defp field_set_key({:upkeep_event, event, values}) do
    {{:upkeep_event, event}, field_names(values)}
  end

  defp field_set_key(_key), do: nil

  defp field_names(values) do
    values
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort()
    |> List.to_tuple()
  end

  defp field_key(_notification, {}, _fields), do: nil

  defp field_key(notification, field_names, fields) do
    values =
      field_names
      |> Tuple.to_list()
      |> Enum.reduce_while([], fn field, values ->
        case Map.fetch(fields, field) do
          {:ok, value} -> {:cont, [{field, value} | values]}
          :error -> {:halt, nil}
        end
      end)

    if is_list(values) do
      values = Enum.sort(values)

      case notification do
        {:upkeep_change, name, schema} -> {:upkeep_change, name, schema, values}
        {:upkeep_event, event} -> {:upkeep_event, event, values}
        _other -> nil
      end
    end
  end
end
