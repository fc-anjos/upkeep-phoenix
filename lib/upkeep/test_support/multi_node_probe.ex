defmodule Upkeep.TestSupport.MultiNodeProbe do
  @moduledoc false

  @doc """
  Return the set of nodes whose pids are members of the upkeep
  notification group from the caller's view.
  """
  def notification_group_node_view do
    Upkeep.Coordinator.Graph.group()
    |> Group.members(Upkeep.Coordinator.Graph.notification_key())
    |> Enum.map(fn {pid, _meta} -> node(pid) end)
    |> Enum.uniq()
  end

  @doc """
  Start a long-lived graph subscriber on the current node.

  The subscriber registers through the public graph API, forwards any
  `{:dag_values, pairs}` messages to `parent`, and stays alive until it receives
  `:stop`. This lets multi-node tests prove the full remote local-index and DAG
  value dispatch path without serializing test-module closures to the peer.
  """
  def start_graph_subscriber(parent, node_id, interest_key, value) do
    pid =
      spawn(fn ->
        :ok =
          Upkeep.Coordinator.Graph.register_loader(node_id, [interest_key], fn ->
            {value, [interest_key]}
          end)

        send(parent, {:peer_graph_subscriber_registered, node(), self(), node_id})
        graph_subscriber_loop(parent)
      end)

    {:ok, pid}
  end

  defp graph_subscriber_loop(parent) do
    receive do
      {:dag_values, pairs} ->
        send(parent, {:peer_dag_values, node(), self(), pairs})
        graph_subscriber_loop(parent)

      :stop ->
        :ok
    end
  end
end
