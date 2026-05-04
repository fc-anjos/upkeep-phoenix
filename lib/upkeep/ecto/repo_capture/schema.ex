defmodule Upkeep.Ecto.RepoCapture.Schema do
  @moduledoc false

  def queryable_schema(queryable, capture_opts) do
    case Keyword.get(capture_opts, :schema) do
      nil -> queryable_source_schema(queryable)
      schema -> schema
    end
  end

  def source_schema(source, capture_opts) do
    case Keyword.get(capture_opts, :schema) do
      nil -> source_schema(source)
      schema -> schema
    end
  end

  def source_schema({source, nil}) when is_binary(source), do: source
  def source_schema({_source, schema}) when is_atom(schema) and not is_nil(schema), do: schema
  def source_schema(schema) when is_atom(schema), do: schema
  def source_schema(source) when is_binary(source), do: source
  def source_schema(_source), do: nil

  def record_schema(%schema{}), do: schema
  def record_schema(_record), do: nil

  def put_query_schema(%Ecto.Query{from: %{source: {source, nil}} = from} = query, schema)
      when is_atom(schema) and not is_nil(schema) do
    %{query | from: %{from | source: {source, schema}}}
  end

  def put_query_schema(query, _schema), do: query

  defp queryable_source_schema(queryable) do
    queryable
    |> Ecto.Queryable.to_query()
    |> Map.get(:from)
    |> then(fn
      %{source: source} -> source_schema(source)
      _from -> nil
    end)
  rescue
    _ -> nil
  end
end
