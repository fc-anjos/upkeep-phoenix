defmodule Upkeep.Runtime.State do
  @moduledoc false

  alias Upkeep.Internal.DAG.Store

  defstruct watches: %{},
            store: nil,
            assign_nodes: %{},
            shared_derived_nodes: %{},
            derive_sharing: %{},
            pending_refreshes: nil

  def new do
    %__MODULE__{
      store: Store.new(),
      pending_refreshes: MapSet.new()
    }
  end

  def fetch(socket) do
    case socket.private do
      %{upkeep_runtime: %__MODULE__{} = runtime} -> runtime
      _ -> new()
    end
  end

  def put(socket, %__MODULE__{} = runtime) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_runtime, runtime)}
  end

  def put_watch(socket, source_id, watch) do
    watch = Map.put(watch, :source_id, source_id)
    runtime = fetch(socket)
    watches = Map.put(runtime.watches, source_id, watch)

    put(socket, %{runtime | watches: watches})
  end

  def put_watch_assign(socket, source_id, assign_name) do
    runtime = fetch(socket)

    watches =
      Map.update!(runtime.watches, source_id, fn watch ->
        Map.update!(watch, :assign_names, &MapSet.put(&1, assign_name))
      end)

    put(socket, %{runtime | watches: watches})
  end

  def put_existing_watch(socket, source_id, watch) do
    runtime = fetch(socket)
    watches = Map.put(runtime.watches, source_id, watch)

    put(socket, %{runtime | watches: watches})
  end

  def put_watches(socket, watches) when is_map(watches) do
    runtime = fetch(socket)
    put(socket, %{runtime | watches: watches})
  end

  def watches(socket) do
    fetch(socket).watches
  end

  def put_store(socket, store) do
    runtime = fetch(socket)
    put(socket, %{runtime | store: store})
  end

  def store(socket) do
    fetch(socket).store
  end

  def put_assign_node(socket, assign_name, node_id) do
    runtime = fetch(socket)
    assign_nodes = Map.put(runtime.assign_nodes, assign_name, node_id)

    put(socket, %{runtime | assign_nodes: assign_nodes})
  end

  def delete_assign_node(socket, assign_name) do
    runtime = fetch(socket)
    assign_nodes = Map.delete(runtime.assign_nodes, assign_name)

    put(socket, %{runtime | assign_nodes: assign_nodes})
  end

  def assign_nodes(socket) do
    fetch(socket).assign_nodes
  end

  def assign_names_for_node(socket, node_id) do
    socket
    |> assign_nodes()
    |> Enum.filter(fn {_assign_name, assigned_node_id} -> assigned_node_id == node_id end)
    |> Enum.map(fn {assign_name, _node_id} -> assign_name end)
  end

  def put_shared_derived_node(socket, node_id, graph_node_id) do
    runtime = fetch(socket)

    shared_derived_nodes =
      Map.put(runtime.shared_derived_nodes, node_id, graph_node_id)

    put(socket, %{runtime | shared_derived_nodes: shared_derived_nodes})
  end

  def shared_derived_nodes(socket) do
    fetch(socket).shared_derived_nodes
  end

  def local_shared_derived_node(socket, graph_node_id) do
    socket
    |> shared_derived_nodes()
    |> Enum.find_value(fn
      {local_node_id, ^graph_node_id} -> local_node_id
      _other -> nil
    end)
  end

  def put_derive_sharing(socket, node_id, metadata) when is_map(metadata) do
    runtime = fetch(socket)
    derive_sharing = Map.put(runtime.derive_sharing, node_id, metadata)

    put(socket, %{runtime | derive_sharing: derive_sharing})
  end

  def derive_sharing(socket) do
    fetch(socket).derive_sharing
  end

  def queue_refresh(socket, source_id) do
    runtime = fetch(socket)
    pending = MapSet.put(runtime.pending_refreshes, source_id)

    put(socket, %{runtime | pending_refreshes: pending})
  end

  def pending_refreshes(socket) do
    fetch(socket).pending_refreshes
  end

  def clear_pending_refreshes(socket) do
    runtime = fetch(socket)
    put(socket, %{runtime | pending_refreshes: MapSet.new()})
  end

  def delete_pending_refresh(socket, source_id) do
    runtime = fetch(socket)
    pending = MapSet.delete(runtime.pending_refreshes, source_id)

    put(socket, %{runtime | pending_refreshes: pending})
  end
end
