defmodule Upkeep.Runtime.StateTest do
  use ExUnit.Case, async: true

  alias Upkeep.Runtime.State

  test "stores Upkeep runtime as one socket-private value" do
    socket =
      new_socket()
      |> State.put_watch(:source_id, %{assign_names: MapSet.new([:items])})
      |> State.put_assign_node(:items, {:source, :source_id})
      |> State.queue_refresh(:source_id)

    assert %State{} = runtime = socket.private.upkeep_runtime
    assert Map.has_key?(socket.private, :upkeep_runtime)
    refute Map.has_key?(socket.private, :upkeep_watches)
    refute Map.has_key?(socket.private, :upkeep_assign_nodes)
    refute Map.has_key?(socket.private, :upkeep_pending_refreshes)
    assert Map.has_key?(runtime.watches, :source_id)
    assert runtime.assign_nodes.items == {:source, :source_id}
    assert MapSet.member?(runtime.pending_refreshes, :source_id)
  end

  test "returns empty runtime state for untouched sockets" do
    socket = new_socket()

    assert State.watches(socket) == %{}
    assert State.assign_nodes(socket) == %{}
    assert State.pending_refreshes(socket) == MapSet.new()
    assert %Upkeep.DAG.Store{} = State.store(socket)
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
end
