defmodule Upkeep.Coordinator.Graph.Shard.Dispatch do
  @moduledoc false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Node
  alias Upkeep.DAG.Store

  def batch(_state, []), do: :ok

  def batch(state, pairs) do
    metadata = %{
      shard: state.idx,
      pair_count: length(pairs),
      node_partitions:
        Enum.map(pairs, fn {node_id, _value} -> {node_id, Graph.node_partition(node_id)} end)
    }

    :telemetry.span([:upkeep, :graph, :dispatch], metadata, fn ->
      # Value fanout deliberately batches per subscriber. Group.dispatch/3 is
      # ideal for single-key notification fanout, but DAG updates can contain
      # source and derived values that should arrive atomically for a LiveView.
      pairs_by_pid =
        Enum.reduce(pairs, %{}, fn {node_id, value}, acc ->
          %Node{encoded_key: encoded_key} = Store.fetch_metadata!(state.store, node_id)

          Graph.group()
          |> Group.members(encoded_key)
          |> Enum.reduce(acc, fn {pid, _meta}, acc ->
            Map.update(acc, pid, [{node_id, value}], &[{node_id, value} | &1])
          end)
        end)

      pid_count = map_size(pairs_by_pid)

      Enum.each(pairs_by_pid, fn {pid, batched} ->
        send(pid, {:dag_values, Enum.reverse(batched)})
      end)

      {:ok, Map.put(metadata, :pid_count, pid_count)}
    end)
  end
end
