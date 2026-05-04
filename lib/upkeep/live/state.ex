defmodule Upkeep.Live.State do
  @moduledoc false

  def put_watch(socket, source_id, watch) do
    private = socket.private || %{}
    watch = Map.put(watch, :source_id, source_id)
    watches = Map.put(Map.get(private, :upkeep_watches, %{}), source_id, watch)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  def put_watch_assign(socket, source_id, assign_name) do
    private = socket.private || %{}

    watches =
      Map.update!(Map.get(private, :upkeep_watches, %{}), source_id, fn watch ->
        Map.update!(watch, :assign_names, &MapSet.put(&1, assign_name))
      end)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  def put_existing_watch(socket, source_id, watch) do
    private = socket.private || %{}
    watches = Map.put(Map.get(private, :upkeep_watches, %{}), source_id, watch)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  def watches(socket) do
    case socket.private do
      %{upkeep_watches: watches} -> watches
      _ -> %{}
    end
  end

  def put_dag(socket, dag) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_dag, dag)}
  end

  def dag(socket) do
    case socket.private do
      %{upkeep_dag: dag} -> dag
      _ -> Upkeep.DAG.new()
    end
  end

  def put_assign_node(socket, assign_name, node_id) do
    private = socket.private || %{}
    assign_nodes = Map.put(Map.get(private, :upkeep_assign_nodes, %{}), assign_name, node_id)

    %{socket | private: Map.put(private, :upkeep_assign_nodes, assign_nodes)}
  end

  def delete_assign_node(socket, assign_name) do
    private = socket.private || %{}
    assign_nodes = Map.delete(Map.get(private, :upkeep_assign_nodes, %{}), assign_name)

    %{socket | private: Map.put(private, :upkeep_assign_nodes, assign_nodes)}
  end

  def assign_nodes(socket) do
    case socket.private do
      %{upkeep_assign_nodes: assign_nodes} -> assign_nodes
      _ -> %{}
    end
  end

  def assign_names_for_node(socket, node_id) do
    socket
    |> assign_nodes()
    |> Enum.filter(fn {_assign_name, assigned_node_id} -> assigned_node_id == node_id end)
    |> Enum.map(fn {assign_name, _node_id} -> assign_name end)
  end

  def queue_refresh(socket, source_id) do
    private = socket.private || %{}
    pending = MapSet.put(Map.get(private, :upkeep_pending_refreshes, MapSet.new()), source_id)

    %{socket | private: Map.put(private, :upkeep_pending_refreshes, pending)}
  end

  def pending_refreshes(socket) do
    case socket.private do
      %{upkeep_pending_refreshes: pending} -> pending
      _ -> MapSet.new()
    end
  end

  def clear_pending_refreshes(socket) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_pending_refreshes, MapSet.new())}
  end

  def delete_pending_refresh(socket, source_id) do
    private = socket.private || %{}
    pending = MapSet.delete(Map.get(private, :upkeep_pending_refreshes, MapSet.new()), source_id)

    %{socket | private: Map.put(private, :upkeep_pending_refreshes, pending)}
  end
end
