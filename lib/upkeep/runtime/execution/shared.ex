defmodule Upkeep.Runtime.Execution.Shared do
  @moduledoc false

  alias Upkeep.DAG.Store
  alias Upkeep.Runtime.State

  def initial_value(
        socket,
        assign_name,
        dep_node_ids,
        _dep_pairs,
        _fun,
        compute,
        _source_location \\ nil
      ) do
    metadata =
      socket
      |> sharing_metadata(assign_name, dep_node_ids)
      |> Map.merge(%{result: :local, reason: :source_process_runtime})

    {compute_initial_value(socket, dep_node_ids, compute), metadata}
  end

  def sharing_plan(_socket, _dep_node_ids, metadata) when is_map(metadata) do
    %{
      final_result: Map.get(metadata, :result),
      final_reason: Map.get(metadata, :reason),
      roots: [],
      candidate_shareable_nodes: [],
      candidate_shareable_subgraphs: [],
      largest_shareable_subgraphs: [],
      boundaries: []
    }
  end

  defp sharing_metadata(socket, assign_name, dep_node_ids) do
    %{
      assign_name: assign_name,
      view: Map.get(socket, :view),
      dep_node_ids: dep_node_ids
    }
  end

  defp compute_initial_value(socket, dep_node_ids, compute) do
    store = State.store(socket)

    dep_node_ids
    |> Map.new(fn id -> {id, Store.fetch!(store, id)} end)
    |> compute.()
  end
end
