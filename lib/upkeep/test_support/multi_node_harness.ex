defmodule Upkeep.TestSupport.MultiNodeHarness do
  @moduledoc false

  alias Upkeep.Coordinator.Graph

  def notification_group_node_view do
    Graph.group()
    |> Group.members(Upkeep.Invalidation.notification_key())
    |> Enum.map(fn {pid, _meta} -> node(pid) end)
    |> Enum.uniq()
  end

  def seed_read_node(node_id, deps, value \\ []) do
    Upkeep.Invalidation.fetch_read(node_id, deps, fn -> value end)
    :ok
  end

  def start_graph_subscriber(parent, node_id, event_name, value) do
    surface =
      Upkeep.InvalidationSurface.manual([{:upkeep_change, event_name, nil}], fn
        %Upkeep.Change{name: ^event_name} -> true
        _event -> false
      end)

    pid =
      spawn(fn ->
        :ok =
          Graph.register_loader(node_id, surface, fn ->
            {value, surface}
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
