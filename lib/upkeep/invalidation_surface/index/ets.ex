defmodule Upkeep.InvalidationSurface.Index.ETS do
  @moduledoc false

  alias Upkeep.InvalidationSurface.Index

  def insert(table, id, surface, payload \\ nil) do
    surface
    |> Index.surface_terms()
    |> Enum.each(&insert_term(table, id, &1, payload))

    :ok
  end

  def replace(table, id, surface, payload \\ nil) do
    delete(table, id)
    insert(table, id, surface, payload)
  end

  def delete(table, id) do
    :ets.match_delete(table, {:_, {id, :_}})
    :ok
  end

  def candidates(table, event) when is_struct(event) do
    {query_terms, lookup_terms} = Index.lookup_terms(event, &field_names(table, &1))

    candidates =
      lookup_terms
      |> Enum.flat_map(&:ets.lookup(table, &1))
      |> Enum.map(fn {_term, candidate} -> candidate end)
      |> Enum.uniq_by(fn {id, _payload} -> id end)

    {query_terms, candidates}
  end

  defp insert_term(table, id, {:field_names, notification, field_names}, _payload) do
    :ets.insert(table, {{:field_names, notification}, {id, field_names}})
  end

  defp insert_term(table, id, term, payload) do
    :ets.insert(table, {term, {id, payload}})
  end

  defp field_names(table, notification) do
    table
    |> :ets.lookup({:field_names, notification})
    |> Enum.map(fn {_term, {_id, field_names}} -> field_names end)
    |> Enum.uniq()
  end
end
