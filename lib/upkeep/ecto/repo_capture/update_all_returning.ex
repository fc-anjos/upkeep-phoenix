defmodule Upkeep.Ecto.RepoCapture.UpdateAllReturning do
  @moduledoc false

  import Ecto.Query

  alias Upkeep.Ecto.RepoCapture.{Schema, TableMetadata}

  def run(repo, queryable, updates, opts, schema)
      when is_atom(repo) and is_list(opts) do
    with {:ok, query} <- returning_query(repo, queryable, schema),
         [_ | _] = changed_fields <- changed_fields(query, updates),
         :ok <- ensure_compiles(repo, query, updates),
         {count, records} <- repo.update_all(query, updates, Keyword.put(opts, :upkeep, false)) do
      {:ok, {count, nil}, List.wrap(records), changed_fields}
    else
      :unsupported -> :unsupported
      [] -> :unsupported
    end
  end

  defp returning_query(_repo, _queryable, nil), do: :unsupported

  defp returning_query(_repo, queryable, schema) when is_atom(schema) do
    query =
      queryable
      |> Ecto.Queryable.to_query()
      |> Schema.put_query_schema(schema)

    if query.select do
      :unsupported
    else
      {:ok, select(query, [record], record)}
    end
  end

  defp returning_query(repo, queryable, source) when is_binary(source) do
    query = Ecto.Queryable.to_query(queryable)
    fields = TableMetadata.fields(repo, source)

    cond do
      query.select -> :unsupported
      fields == [] -> :unsupported
      true -> {:ok, select(query, [record], map(record, ^fields))}
    end
  end

  defp returning_query(_repo, _queryable, _schema), do: :unsupported

  defp changed_fields(query, updates) do
    query
    |> update_query(updates)
    |> Map.get(:updates)
    |> Enum.flat_map(fn %{expr: expr} ->
      for {_op, kw} <- expr, {field, _value} <- kw, is_atom(field), do: field
    end)
    |> Enum.uniq()
  end

  defp ensure_compiles(repo, query, updates) do
    if function_exported?(repo, :to_sql, 2) do
      :update_all
      |> Ecto.Adapters.SQL.to_sql(repo, update_query(query, updates))
      |> case do
        {_sql, _params} -> :ok
      end
    else
      :unsupported
    end
  rescue
    exception ->
      if unsupported_returning_error?(exception) do
        :unsupported
      else
        reraise exception, __STACKTRACE__
      end
  end

  defp update_query(query, []), do: query
  defp update_query(query, updates), do: from(record in query, update: ^updates)

  defp unsupported_returning_error?(%Ecto.QueryError{message: message}),
    do:
      String.contains?(message, "select is not supported") and
        String.contains?(message, "update_all")

  defp unsupported_returning_error?(%ArgumentError{message: message}),
    do:
      String.contains?(message, "select is not supported") and
        String.contains?(message, "update_all")

  defp unsupported_returning_error?(_exception), do: false
end
