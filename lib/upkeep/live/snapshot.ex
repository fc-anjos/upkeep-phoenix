defmodule Upkeep.Live.Snapshot do
  @moduledoc false

  alias Upkeep.DAG.{Graph, Store}
  alias Upkeep.Live.{Ids, Telemetry}
  alias Upkeep.Runtime.State
  alias Upkeep.Source.Runtime, as: Source

  def build(socket) do
    %{
      dag: Graph.snapshot(Store.graph(State.store(socket))),
      assigns: assign_snapshot(socket),
      watches: watch_snapshot(socket),
      pending_refreshes: pending_refresh_snapshot(socket)
    }
  end

  defp assign_snapshot(socket) do
    derive_sharing = State.derive_sharing(socket)

    socket
    |> State.assign_nodes()
    |> Enum.map(fn {assign_name, node_id} ->
      %{assign: assign_name, node_id: node_id}
      |> maybe_put_sharing(Map.get(derive_sharing, node_id))
    end)
    |> sort_maps_by(:assign)
  end

  defp maybe_put_sharing(assign, nil), do: assign
  defp maybe_put_sharing(assign, sharing), do: Map.put(assign, :sharing, sharing)

  defp watch_snapshot(socket) do
    socket
    |> State.watches()
    |> Enum.map(fn {source_id, watch} ->
      %{
        source_id: source_id,
        node_id: Ids.source_node_id(source_id),
        source: watch.source,
        params: watch.params,
        sharing_partition: Source.sharing_partition(watch.source, watch.params),
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
