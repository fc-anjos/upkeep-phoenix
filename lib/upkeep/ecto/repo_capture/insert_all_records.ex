defmodule Upkeep.Ecto.RepoCapture.InsertAllRecords do
  @moduledoc false

  def query_entries(repo, query, schema) do
    with :ok <- ensure_schema(schema) do
      query
      |> repo.all()
      |> submitted_entries(schema)
    end
  end

  def submitted_entries(entries, schema) do
    with :ok <- ensure_schema(schema),
         true <- is_list(entries) do
      records(entries, schema)
    else
      false -> {:deopt, :unmaterializable_entries}
      {:deopt, reason} -> {:deopt, reason}
    end
  end

  def materialize(_result, nil, _entries), do: {:deopt, :no_schema}

  def materialize(result, schema, {:ok, entries}) do
    case returning_records(result) do
      {:ok, [_ | _] = records} -> merge_returning_records(schema, records, entries)
      {:ok, []} -> records(entries, schema)
      :none -> records(entries, schema)
      :invalid -> {:deopt, :unmaterializable_entries}
    end
  end

  def materialize(result, schema, {:deopt, _reason}) do
    case returning_records(result) do
      {:ok, [_ | _] = records} -> records(records, schema)
      {:ok, []} -> {:deopt, :no_returning_records}
      :none -> {:deopt, :no_returning_records}
      :invalid -> {:deopt, :unmaterializable_entries}
    end
  end

  defp ensure_schema(nil), do: {:deopt, :no_schema}
  defp ensure_schema(schema) when is_atom(schema) or is_binary(schema), do: :ok
  defp ensure_schema(_schema), do: {:deopt, :no_schema}

  defp returning_records({_count, records}) when is_list(records), do: {:ok, records}
  defp returning_records({_count, nil}), do: :none
  defp returning_records({_count, _records}), do: :invalid
  defp returning_records(_result), do: :none

  defp records(entries, schema) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, records} ->
      case record(schema, entry) do
        {:ok, record} -> {:cont, {:ok, [record | records]}}
        {:deopt, reason} -> {:halt, {:deopt, reason}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:deopt, reason} -> {:deopt, reason}
    end
  end

  defp record(schema, entry) when is_binary(schema) do
    case entry_map(entry) do
      {:ok, map} -> {:ok, map}
      :error -> {:deopt, :unmaterializable_entries}
    end
  end

  defp record(schema, %schema{} = entry) when is_atom(schema), do: {:ok, entry}

  defp record(schema, entry) when is_atom(schema) do
    case entry_map(entry) do
      {:ok, map} -> {:ok, struct(schema, map)}
      :error -> {:deopt, :unmaterializable_entries}
    end
  rescue
    ArgumentError -> {:deopt, :unmaterializable_entries}
  end

  defp merge_returning_records(schema, records, entries) do
    case merge_returning_records(records, entries, schema, []) do
      {:ok, merged_records} -> {:ok, merged_records}
      {:deopt, reason} -> {:deopt, reason}
      :length_mismatch -> records(records, schema)
    end
  end

  defp merge_returning_records([], [], _schema, merged_records),
    do: {:ok, Enum.reverse(merged_records)}

  defp merge_returning_records([record | records], [entry | entries], schema, merged_records) do
    case merge_returning_record(schema, record, entry) do
      {:ok, record} ->
        merge_returning_records(records, entries, schema, [record | merged_records])

      {:deopt, reason} ->
        {:deopt, reason}
    end
  end

  defp merge_returning_records(_records, _entries, _schema, _merged_records),
    do: :length_mismatch

  defp merge_returning_record(schema, record, entry) do
    with {:ok, record_map} <- entry_map(record),
         {:ok, entry_map} <- entry_map(entry) do
      merged =
        Map.merge(entry_map, record_map, fn _key, entry_value, record_value ->
          returning_value(entry_value, record_value)
        end)

      record(schema, merged)
    else
      :error -> {:deopt, :unmaterializable_entries}
    end
  end

  defp returning_value(entry_value, nil), do: entry_value
  defp returning_value(_entry_value, record_value), do: record_value

  defp entry_map(%_{} = entry), do: {:ok, Map.from_struct(entry)}
  defp entry_map(entry) when is_map(entry), do: {:ok, entry}

  defp entry_map(entry) when is_list(entry) do
    Enum.reduce_while(entry, {:ok, %{}}, fn
      {key, value}, {:ok, map} -> {:cont, {:ok, Map.put(map, key, value)}}
      _value, _acc -> {:halt, :error}
    end)
  end

  defp entry_map(_entry), do: :error
end
