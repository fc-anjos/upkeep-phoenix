defmodule Upkeep.Live.Inspector.Activity do
  @moduledoc false

  def from_document(document) do
    latest_recompute =
      document.events
      |> Enum.reverse()
      |> Enum.find(&(&1.name == [:upkeep, :dag, :recompute, :stop]))

    roots = metadata_set(latest_recompute, :changed_source_nodes)
    recomputed = metadata_set(latest_recompute, :recomputed_nodes)
    changed = metadata_set(latest_recompute, :changed_derived_nodes)
    skipped = metadata_set(latest_recompute, :skipped_nodes)

    %{
      latest_recompute: latest_recompute,
      roots: roots,
      recomputed: recomputed,
      changed: changed,
      skipped: skipped,
      steps: steps(recomputed),
      scopes: node_scopes(document)
    }
  end

  def watch_scope(%{component: nil}), do: :shared
  def watch_scope(_watch), do: :local

  defp metadata_set(nil, _key), do: MapSet.new()

  defp metadata_set(event, key) do
    event.metadata
    |> Map.get(key, [])
    |> List.wrap()
    |> MapSet.new()
  end

  defp steps(set) do
    set
    |> MapSet.to_list()
    |> Enum.with_index(1)
    |> Map.new()
  end

  defp node_scopes(document) do
    source_scopes =
      Map.new(document.watches, fn watch ->
        {watch.node_id, watch_scope(watch)}
      end)

    assign_scopes =
      document.assigns
      |> Enum.filter(&Map.has_key?(&1, :sharing))
      |> Map.new(fn assign ->
        {assign.node_id, Map.get(assign.sharing, :result, :unknown)}
      end)

    Map.merge(source_scopes, assign_scopes)
  end
end
