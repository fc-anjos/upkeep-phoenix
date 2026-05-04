defmodule Upkeep.Live.Snapshot do
  @moduledoc false

  alias Upkeep.Live.{Ids, State, Telemetry}

  def build(socket) do
    %{
      dag: Upkeep.DAG.snapshot(State.dag(socket)),
      assigns: assign_snapshot(socket),
      watches: watch_snapshot(socket),
      pending_refreshes: pending_refresh_snapshot(socket)
    }
  end

  defp assign_snapshot(socket) do
    socket
    |> State.assign_nodes()
    |> Enum.map(fn {assign_name, node_id} -> %{assign: assign_name, node_id: node_id} end)
    |> sort_maps_by(:assign)
  end

  defp watch_snapshot(socket) do
    socket
    |> State.watches()
    |> Enum.map(fn {source_id, watch} ->
      %{
        source_id: source_id,
        node_id: Ids.source_node_id(source_id),
        source: watch.source,
        params: watch.params,
        component: watch.component,
        assign_names: watch.assign_names |> MapSet.to_list() |> Telemetry.sort_terms(),
        interest_keys: Telemetry.sort_terms(watch.interest_keys)
      }
    end)
    |> sort_maps_by(:source_id)
  end

  defp pending_refresh_snapshot(socket) do
    socket
    |> State.pending_refreshes()
    |> MapSet.to_list()
    |> Telemetry.sort_terms()
  end

  defp sort_maps_by(maps, key), do: Enum.sort_by(maps, &inspect(Map.fetch!(&1, key)))
end
