defmodule Upkeep.Ecto.RepoCapture.TableMetadata do
  @moduledoc false

  def metadata(repo, source) do
    case repo.__adapter__() do
      Ecto.Adapters.Postgres -> {:ok, postgres_metadata(repo, source)}
      adapter when is_atom(adapter) -> adapter_metadata(repo, source, adapter)
    end
  rescue
    _exception -> {:deopt, :table_metadata_failed}
  end

  defp adapter_metadata(repo, source, adapter) do
    if sqlite_adapter?(adapter) do
      {:ok, sqlite_metadata(repo, source)}
    else
      {:deopt, :unsupported_adapter}
    end
  end

  defp sqlite_adapter?(adapter) do
    Atom.to_string(adapter) == "Elixir.Ecto.Adapters.SQLite3"
  end

  defp sqlite_metadata(repo, source) do
    source = quoted_sqlite_identifier(source)
    result = repo.query!("PRAGMA table_info(#{source})", [])

    fields =
      Enum.map(result.rows, fn [_cid, name | _rest] ->
        String.to_atom(name)
      end)

    primary_keys =
      result.rows
      |> Enum.filter(fn [_cid, _name, _type, _not_null, _default, pk] -> pk > 0 end)
      |> Enum.sort_by(fn [_cid, _name, _type, _not_null, _default, pk] -> pk end)
      |> Enum.map(fn [_cid, name, _type, _not_null, _default, _pk] -> String.to_atom(name) end)

    %{fields: fields, primary_keys: primary_keys}
  end

  defp postgres_metadata(repo, source) do
    result =
      repo.query!(
        """
        SELECT column_name, ordinal_position,
               EXISTS (
                 SELECT 1
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                  AND tc.table_name = kcu.table_name
                 WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_schema = c.table_schema
                   AND tc.table_name = c.table_name
                   AND kcu.column_name = c.column_name
               ) AS primary_key
        FROM information_schema.columns c
        WHERE c.table_schema = current_schema()
          AND c.table_name = $1
        ORDER BY ordinal_position
        """,
        [source]
      )

    fields = Enum.map(result.rows, fn [name, _position, _primary_key] -> String.to_atom(name) end)

    primary_keys =
      result.rows
      |> Enum.filter(fn [_name, _position, primary_key] -> primary_key end)
      |> Enum.map(fn [name, _position, _primary_key] -> String.to_atom(name) end)

    %{fields: fields, primary_keys: primary_keys}
  end

  defp quoted_sqlite_identifier(source) do
    ~s("#{String.replace(source, ~s("), ~s(""))}")
  end
end
