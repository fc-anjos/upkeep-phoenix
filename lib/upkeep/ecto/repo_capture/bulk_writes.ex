defmodule Upkeep.Ecto.RepoCapture.BulkWrites do
  @moduledoc false

  alias Upkeep.Ecto.RepoCapture.{BulkRows, InsertAllRecords, Notify, Schema, UpdateAllReturning}

  def capture_insert_all(repo, schema_or_source, %Ecto.Query{} = query, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    case repo.transaction(fn ->
           schema = Schema.source_schema(schema_or_source, capture_opts)
           entries = InsertAllRecords.query_entries(repo, query, schema)
           result = run.()

           notify_insert_all(repo, result, schema, entries)

           result
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  def capture_insert_all(repo, schema_or_source, entries, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    schema = Schema.source_schema(schema_or_source, capture_opts)
    entries = InsertAllRecords.submitted_entries(entries, schema)
    result = run.()

    notify_insert_all(repo, result, schema, entries)

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
           update_with_loaded_rows(repo, queryable, schema, run)
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp update_with_loaded_rows(repo, queryable, schema, run) do
    case BulkRows.load(repo, queryable, schema) do
      {:ok, before_records} ->
        notify_update_all_with_reloaded_records(repo, schema, before_records, run)

      {:deopt, reason} ->
        emit_bulk_capture_deopt(repo, schema, :update_all, reason)
        result = run.()
        Notify.notify_bulk_deopt(:updated, schema)
        result
    end
  end

  defp notify_update_all_with_reloaded_records(repo, schema, before_records, run) do
    result = run.()
    after_records = BulkRows.reload(repo, schema, before_records)

    before_records
    |> Enum.zip(after_records)
    |> Enum.each(fn {before_record, after_record} ->
      Notify.notify_change(:updated, schema, after_record, before_record)
    end)

    result
  end

  def capture_delete_all(repo, queryable, capture_opts, run)
      when is_atom(repo) and is_function(run, 0) do
    schema = Schema.queryable_schema(queryable, capture_opts)

    case repo.transaction(fn ->
           delete_loaded_rows(repo, queryable, schema, run)
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_loaded_rows(repo, queryable, schema, run) do
    case BulkRows.load(repo, queryable, schema) do
      {:ok, before_records} ->
        result = run.()
        Enum.each(before_records, &Notify.notify_change(:deleted, schema, &1))
        result

      {:deopt, reason} ->
        emit_bulk_capture_deopt(repo, schema, :delete_all, reason)
        result = run.()
        Notify.notify_bulk_deopt(:deleted, schema)
        result
    end
  end

  defp emit_bulk_capture_deopt(repo, schema, operation, reason) do
    :telemetry.execute(
      [:upkeep, :repo, :bulk_capture, :deopt],
      %{count: 1},
      %{repo: repo, schema: schema, operation: operation, reason: reason}
    )
  end

  defp notify_insert_all(repo, result, schema, entries) do
    case InsertAllRecords.materialize(result, schema, entries) do
      {:ok, records} ->
        Enum.each(records, &Notify.notify_change(:inserted, schema, &1))

      {:deopt, reason} ->
        emit_bulk_capture_deopt(repo, schema, :insert_all, reason)
        Notify.notify_bulk_deopt(:inserted, schema)
    end
  end
end
