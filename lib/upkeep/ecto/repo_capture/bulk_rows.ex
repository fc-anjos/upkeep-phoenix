defmodule Upkeep.Ecto.RepoCapture.BulkRows do
  @moduledoc false

  import Ecto.Query

  alias Upkeep.Ecto.RepoCapture.{Schema, TableMetadata}

  def load(repo, queryable, schema) do
    case Schema.capture_query(repo, queryable, schema) do
      {:ok, query} -> {:ok, repo.all(query)}
      {:deopt, reason} -> {:deopt, reason}
    end
  end

  def reload(_repo, nil, _before_records), do: []
  def reload(_repo, _schema, []), do: []

  def reload(repo, schema, before_records) when is_atom(schema) do
    case schema.__schema__(:primary_key) do
      [] ->
        before_records

      primary_keys ->
        schema
        |> primary_key_query(primary_keys, before_records)
        |> repo.all()
        |> reload_records_by_key(before_records, primary_keys)
    end
  end

  def reload(repo, source, before_records) when is_binary(source) do
    fields = before_records |> List.first() |> Map.keys()

    case TableMetadata.metadata(repo, source) do
      {:ok, %{primary_keys: []}} ->
        before_records

      {:ok, %{primary_keys: _primary_keys}} when fields == [] ->
        before_records

      {:ok, %{primary_keys: primary_keys}} ->
        source
        |> primary_key_query(primary_keys, before_records, fields)
        |> repo.all()
        |> reload_records_by_key(before_records, primary_keys)

      {:deopt, _reason} ->
        before_records
    end
  end

  defp primary_key_query(schema, [primary_key], records) do
    values = Enum.map(records, &Map.fetch!(&1, primary_key))

    from record in schema,
      where: field(record, ^primary_key) in ^values
  end

  defp primary_key_query(schema, primary_keys, records) do
    predicate = primary_key_predicate(records, primary_keys)

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
    predicate = primary_key_predicate(records, primary_keys)

    from record in source,
      where: ^predicate,
      select: map(record, ^fields)
  end

  defp primary_key_predicate(records, primary_keys) do
    Enum.reduce(records, dynamic(false), fn record, predicate ->
      record_predicate =
        Enum.reduce(primary_keys, dynamic(true), fn primary_key, record_predicate ->
          value = Map.fetch!(record, primary_key)
          dynamic([candidate], ^record_predicate and field(candidate, ^primary_key) == ^value)
        end)

      dynamic([candidate], ^predicate or ^record_predicate)
    end)
  end

  defp primary_key_values(record, primary_keys) do
    Enum.map(primary_keys, &Map.fetch!(record, &1))
  end

  defp reload_records_by_key(after_records, before_records, primary_keys) do
    records_by_key =
      Map.new(after_records, fn record -> {primary_key_values(record, primary_keys), record} end)

    Enum.map(before_records, fn record ->
      Map.get(records_by_key, primary_key_values(record, primary_keys), record)
    end)
  end
end
