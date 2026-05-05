defmodule Upkeep.Source.Reactivity do
  @moduledoc false

  defdelegate event_keys(event), to: Upkeep.Source.Keys

  def deps_interest_keys(deps) do
    deps
    |> Enum.flat_map(&Upkeep.Source.QueryDeps.interest_keys/1)
    |> Enum.uniq()
  end

  def deps_react_to?(deps, event) do
    Enum.any?(deps, &Upkeep.Source.QueryDeps.matches_change?(&1, event))
  end

  def query_interest_keys(source, params) when is_atom(source) do
    source
    |> source_query(params)
    |> Upkeep.Source.QueryDeps.interest_keys()
  end

  def query_reacts_to?(source, event, params) when is_atom(source) and is_struct(event) do
    deps =
      source
      |> source_query(params)
      |> Upkeep.Source.QueryDeps.from_query()

    Upkeep.Source.QueryDeps.matches_change?(deps, event)
  end

  defp source_query(source, params) do
    if function_exported?(source, :query, 1), do: source.query(params), else: nil
  end
end
