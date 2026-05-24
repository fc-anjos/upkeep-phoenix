defmodule Upkeep.Ecto.RepoCapture.Notify do
  @moduledoc false

  alias Upkeep.Ecto.RepoCapture.Schema

  def capture_result({:ok, record} = result, action, input, true) do
    notify(action, record, input)
    result
  end

  def capture_result(result, _action, _input, _capture?), do: result

  def capture_record!(record, action, input, true) do
    notify(action, record, input)
    record
  end

  def capture_record!(record, _action, _input, _capture?), do: record

  def notify(:inserted, record, _input),
    do: notify_change(:inserted, Schema.record_schema(record), record)

  def notify(:deleted, record, _input),
    do: notify_change(:deleted, Schema.record_schema(record), record)

  def notify(:updated, record, input) do
    notify_change(:updated, Schema.record_schema(record), record, before_record(input))
  end

  def notify_change(action, schema, record), do: notify_change(action, schema, record, nil)

  def notify_change(action, schema, record, from),
    do: notify_change(action, schema, record, from, [])

  def notify_change(_action, nil, _record, _from, _opts), do: :ok
  def notify_change(_action, Ecto.Migration.SchemaMigration, _record, _from, _opts), do: :ok

  def notify_change(action, schema, record, from, opts) do
    action
    |> Upkeep.Change.changed(record,
      action: action,
      schema: schema,
      record: record,
      from: from,
      changed_fields: Keyword.get(opts, :changed_fields),
      meta: Keyword.get(opts, :meta, %{})
    )
    |> Upkeep.Mutation.notify()
  end

  @doc """
  Emits a broad, schema/table-wide invalidation for a bulk write that could not
  materialize its affected rows.

  When `update_all`/`delete_all`/`insert_all` deopts terminally (no schema, an
  uninspectable `binary` table source, an adapter without `RETURNING`, a
  caller-supplied `select`, or a table-metadata failure) Upkeep has no field
  knowledge for the affected rows. Rather than silently skip invalidation and
  serve stale reads, it falls back to broad changes with no record, `from`, or
  `changed_fields`:

    * a broad `:updated` change — exactly the shape `Upkeep.updated/1` produces.
      Per `Upkeep.Change.broad_update?/1` this matches every source reading the
      schema/table (field-indexed or not) that reacts to `:updated`, which covers
      Ecto query sources since they watch all of insert/update/delete; and
    * the action-specific change (`:inserted`/`:deleted`) so sources registered
      broadly for just that action also refresh.

  It is intentionally over-broad but sound: a deopted bulk write refreshes every
  source reading the affected table instead of silently missing.
  """
  def notify_bulk_deopt(_action, nil), do: :ok

  def notify_bulk_deopt(:updated, schema) do
    notify_change(:updated, schema, nil, nil, [])
  end

  def notify_bulk_deopt(action, schema) do
    notify_change(:updated, schema, nil, nil, [])
    notify_change(action, schema, nil, nil, [])
  end

  defp before_record(%Ecto.Changeset{data: data}), do: data
  defp before_record(record), do: record
end
