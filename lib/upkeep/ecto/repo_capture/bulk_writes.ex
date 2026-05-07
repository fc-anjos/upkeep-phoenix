defmodule Upkeep.Ecto.RepoCapture.BulkWrites do
  @moduledoc false

  alias Upkeep.Ecto.RepoCapture.{BulkRows, Notify, Schema, UpdateAllReturning}

  def capture_insert_all(repo, schema_or_source, %Ecto.Query{} = query, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = Schema.source_schema(schema_or_source, capture_opts)
           entries = insert_query_entries(repo, query, schema)
           result = run.()

           notify_insert_all(result, schema, entries)

           result
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def capture_insert_all(_repo, schema_or_source, entries, capture_opts, run)
      when is_function(run, 0) do
    schema = Schema.source_schema(schema_or_source, capture_opts)
    result = run.()

    notify_insert_all(result, schema, entries)

    result
  end

  def capture_update_all(repo, queryable, updates, opts, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    schema = Schema.queryable_schema(queryable, capture_opts)

    case UpdateAllReturning.run(repo, queryable, updates, opts, schema) do
      {:ok, result, records, changed_fields} ->
        Enum.each(records, fn record ->
          Notify.notify_change(:updated, schema, record, nil, changed_fields: changed_fields)
        end)

        result

      {:deopt, reason} ->
        emit_update_all_deopt(repo, schema, reason)
        capture_update_all_with_reloaded_records(repo, queryable, schema, run)
    end
  end

  defp emit_update_all_deopt(repo, schema, reason) do
    :telemetry.execute(
      [:upkeep, :repo, :update_all_returning, :deopt],
      %{count: 1},
      %{repo: repo, schema: schema, operation: :update_all, reason: reason}
    )
  end

  defp capture_update_all_with_reloaded_records(repo, queryable, schema, run) do
    case repo.transaction(fn ->
           case BulkRows.load(repo, queryable, schema) do
             {:ok, before_records} ->
               result = run.()
               after_records = BulkRows.reload(repo, schema, before_records)

               before_records
               |> Enum.zip(after_records)
               |> Enum.each(fn {before_record, after_record} ->
                 Notify.notify_change(:updated, schema, after_record, before_record)
               end)

               result

             {:deopt, reason} ->
               emit_bulk_capture_deopt(repo, schema, :update_all, reason)
               run.()
           end
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def capture_delete_all(repo, queryable, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = Schema.queryable_schema(queryable, capture_opts)

           case BulkRows.load(repo, queryable, schema) do
             {:ok, before_records} ->
               result = run.()

               Enum.each(before_records, &Notify.notify_change(:deleted, schema, &1))

               result

             {:deopt, reason} ->
               emit_bulk_capture_deopt(repo, schema, :delete_all, reason)
               run.()
           end
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp emit_bulk_capture_deopt(repo, schema, operation, reason) do
    :telemetry.execute(
      [:upkeep, :repo, :bulk_capture, :deopt],
      %{count: 1},
      %{repo: repo, schema: schema, operation: operation, reason: reason}
    )
  end

  defp notify_insert_all(result, schema, entries) do
    result
    |> inserted_records(schema, entries)
    |> Enum.each(&Notify.notify_change(:inserted, schema, &1))
  end

  defp insert_query_entries(repo, query, schema) do
    query
    |> repo.all()
    |> Enum.map(&entry_record(schema, &1))
  end

  defp inserted_records({_count, records}, schema, entries)
       when is_atom(schema) and is_list(records) and is_list(entries) do
    records
    |> Enum.zip(entries)
    |> Enum.map(fn {record, entry} -> merge_insert_record(schema, record, entry) end)
  end

  defp inserted_records({_count, records}, _schema, _entries) when is_list(records), do: records

  defp inserted_records(_result, schema, entries) when is_atom(schema) and is_list(entries) do
    Enum.map(entries, &entry_record(schema, &1))
  end

  defp inserted_records(_result, schema, entries) when is_binary(schema) and is_list(entries) do
    Enum.map(entries, &entry_record(schema, &1))
  end

  defp inserted_records(_result, _schema, _entries), do: []

  defp entry_record(nil, _entry), do: nil
  defp entry_record(schema, entry) when is_binary(schema), do: entry_map(entry)
  defp entry_record(schema, %schema{} = entry) when is_atom(schema), do: entry

  defp entry_record(schema, %_{} = entry) when is_atom(schema),
    do: struct(schema, Map.from_struct(entry))

  defp entry_record(schema, entry) when is_atom(schema) and is_map(entry),
    do: struct(schema, entry)

  defp entry_record(schema, entry) when is_atom(schema) and is_list(entry),
    do: struct(schema, Map.new(entry))

  defp merge_insert_record(schema, %schema{} = record, entry) do
    entry_map =
      schema
      |> entry_record(entry)
      |> Map.from_struct()

    record_map = Map.from_struct(record)

    schema
    |> struct(
      Map.merge(entry_map, record_map, fn _key, entry_value, record_value ->
        if is_nil(record_value), do: entry_value, else: record_value
      end)
    )
  end

  defp merge_insert_record(schema, record, entry)
       when is_binary(schema) and is_map(record) do
    Map.merge(entry_map(entry), entry_map(record), fn _key, entry_value, record_value ->
      if is_nil(record_value), do: entry_value, else: record_value
    end)
  end

  defp merge_insert_record(_schema, record, _entry), do: record

  defp entry_map(%_{} = entry), do: Map.from_struct(entry)
  defp entry_map(entry) when is_map(entry), do: entry
  defp entry_map(entry) when is_list(entry), do: Map.new(entry)
  defp entry_map(_entry), do: %{}
end
