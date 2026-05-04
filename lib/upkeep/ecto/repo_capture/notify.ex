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

  def notify_change(_action, nil, _record, _from), do: :ok
  def notify_change(_action, Ecto.Migration.SchemaMigration, _record, _from), do: :ok

  def notify_change(action, schema, record, from) do
    action
    |> Upkeep.Change.changed(record,
      action: action,
      schema: schema,
      record: record,
      from: from
    )
    |> Upkeep.Mutation.notify()
  end

  defp before_record(%Ecto.Changeset{data: data}), do: data
  defp before_record(record), do: record
end
