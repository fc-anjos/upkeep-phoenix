defmodule Upkeep.Ecto.RepoCapture.BulkWrites do
  @moduledoc false

  import Ecto.Query

  alias Upkeep.Ecto.RepoCapture.{Notify, Schema, TableMetadata}

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

  def capture_update_all(repo, queryable, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = Schema.queryable_schema(queryable, capture_opts)
           before_records = bulk_records(repo, queryable, schema)
           result = run.()
           after_records = reload_records(repo, schema, before_records)

           before_records
           |> Enum.zip(after_records)
           |> Enum.each(fn {before_record, after_record} ->
             Notify.notify_change(:updated, schema, after_record, before_record)
           end)

           result
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def capture_delete_all(repo, queryable, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = Schema.queryable_schema(queryable, capture_opts)
           before_records = bulk_records(repo, queryable, schema)
           result = run.()

           Enum.each(before_records, &Notify.notify_change(:deleted, schema, &1))

           result
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
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

  defp bulk_records(repo, queryable, schema) do
    case schema do
      nil ->
        []

      _schema ->
        case bulk_select_query(repo, queryable, schema) do
          nil -> []
          query -> repo.all(query)
        end
    end
  end

  defp bulk_select_query(_repo, queryable, schema) when is_atom(schema) do
    queryable
    |> Ecto.Queryable.to_query()
    |> Schema.put_query_schema(schema)
    |> exclude(:select)
    |> select([record], record)
  end

  defp bulk_select_query(repo, queryable, source) when is_binary(source) do
    fields = TableMetadata.fields(repo, source)

    if fields == [] do
      nil
    else
      queryable
      |> Ecto.Queryable.to_query()
      |> exclude(:select)
      |> select([record], map(record, ^fields))
    end
  end

  defp reload_records(_repo, nil, _before_records), do: []
  defp reload_records(_repo, _schema, []), do: []

  defp reload_records(repo, schema, before_records) when is_atom(schema) do
    case schema.__schema__(:primary_key) do
      [] ->
        before_records

      primary_keys ->
        schema
        |> primary_key_query(primary_keys, before_records)
        |> repo.all()
        |> Map.new(fn record -> {primary_key_values(record, primary_keys), record} end)
        |> then(fn records_by_key ->
          Enum.map(before_records, fn record ->
            Map.get(records_by_key, primary_key_values(record, primary_keys), record)
          end)
        end)
    end
  end

  defp reload_records(repo, source, before_records) when is_binary(source) do
    fields = before_records |> List.first() |> Map.keys()

    case TableMetadata.primary_keys(repo, source) do
      [] ->
        before_records

      _primary_keys when fields == [] ->
        before_records

      primary_keys ->
        source
        |> primary_key_query(primary_keys, before_records, fields)
        |> repo.all()
        |> Map.new(fn record -> {primary_key_values(record, primary_keys), record} end)
        |> then(fn records_by_key ->
          Enum.map(before_records, fn record ->
            Map.get(records_by_key, primary_key_values(record, primary_keys), record)
          end)
        end)
    end
  end

  defp primary_key_query(schema, [primary_key], records) do
    values = Enum.map(records, &Map.fetch!(&1, primary_key))

    from record in schema,
      where: field(record, ^primary_key) in ^values
  end

  defp primary_key_query(schema, primary_keys, records) do
    predicate =
      Enum.reduce(records, dynamic(false), fn record, predicate ->
        record_predicate =
          Enum.reduce(primary_keys, dynamic(true), fn primary_key, record_predicate ->
            value = Map.fetch!(record, primary_key)
            dynamic([candidate], ^record_predicate and field(candidate, ^primary_key) == ^value)
          end)

        dynamic([candidate], ^predicate or ^record_predicate)
      end)

    from record in schema,
      where: ^predicate
  end

  defp primary_key_query(source, [primary_key], records, fields) when is_binary(source) do
    values = Enum.map(records, &Map.fetch!(&1, primary_key))

    from record in source,
      where: field(record, ^primary_key) in ^values,
      select: map(record, ^fields)
  end

  defp primary_key_query(source, primary_keys, records, fields) when is_binary(source) do
    predicate =
      Enum.reduce(records, dynamic(false), fn record, predicate ->
        record_predicate =
          Enum.reduce(primary_keys, dynamic(true), fn primary_key, record_predicate ->
            value = Map.fetch!(record, primary_key)
            dynamic([candidate], ^record_predicate and field(candidate, ^primary_key) == ^value)
          end)

        dynamic([candidate], ^predicate or ^record_predicate)
      end)

    from record in source,
      where: ^predicate,
      select: map(record, ^fields)
  end

  defp primary_key_values(record, primary_keys) do
    Enum.map(primary_keys, &Map.fetch!(record, &1))
  end
end
