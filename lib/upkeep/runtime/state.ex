defmodule Upkeep.Runtime.State do
  @moduledoc false

  alias Upkeep.DAG.Store
  alias Upkeep.InvalidationSurface
  alias Upkeep.InvalidationSurface.Index, as: SurfaceIndex

  defstruct watches: %{},
            watch_index: SurfaceIndex.new(),
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
    runtime = runtime |> remove_watch_index(source_id) |> add_watch_index(source_id, watch)

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
    watch = Map.put(watch, :source_id, source_id)
    runtime = fetch(socket)
    watches = Map.put(runtime.watches, source_id, watch)
    runtime = runtime |> remove_watch_index(source_id) |> add_watch_index(source_id, watch)

    put(socket, %{runtime | watches: watches})
  end

  def put_watches(socket, watches) when is_map(watches) do
    runtime = fetch(socket)
    put(socket, %{runtime | watches: watches, watch_index: build_watch_index(watches)})
  end

  def watches(socket) do
    fetch(socket).watches
  end

  def matching_watches(socket, event) when is_struct(event) do
    runtime = fetch(socket)
    source_ids = SurfaceIndex.candidates(runtime.watch_index, event)

    if MapSet.size(source_ids) == map_size(runtime.watches) do
      Map.to_list(runtime.watches)
    else
      Enum.flat_map(source_ids, &fetch_watch(runtime.watches, &1))
    end
  end

  defp fetch_watch(watches, source_id) do
    case Map.fetch(watches, source_id) do
      {:ok, watch} -> [{source_id, watch}]
      :error -> []
    end
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

  def put_shared_derived_node(socket, local_node_id, graph_node_id) do
    runtime = fetch(socket)
    shared_derived_nodes = Map.put(runtime.shared_derived_nodes, local_node_id, graph_node_id)

    put(socket, %{runtime | shared_derived_nodes: shared_derived_nodes})
  end

  def delete_shared_derived_node(socket, local_node_id) do
    runtime = fetch(socket)
    shared_derived_nodes = Map.delete(runtime.shared_derived_nodes, local_node_id)

    put(socket, %{runtime | shared_derived_nodes: shared_derived_nodes})
  end

  def shared_derived_nodes(socket) do
    fetch(socket).shared_derived_nodes
  end

  def shared_derived_graph_node(socket, local_node_id) do
    case Map.fetch(shared_derived_nodes(socket), local_node_id) do
      {:ok, graph_node_id} -> {:ok, graph_node_id}
      :error -> :error
    end
  end

  def local_shared_derived_node(socket, graph_node_id) do
    socket
    |> shared_derived_nodes()
    |> Enum.find_value(:error, fn
      {local_node_id, ^graph_node_id} -> {:ok, local_node_id}
      {_local_node_id, _graph_node_id} -> false
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

  defp add_watch_index(runtime, source_id, watch) do
    watch_index = SurfaceIndex.put(runtime.watch_index, source_id, watch_surface(watch))
    %{runtime | watch_index: watch_index}
  end

  defp remove_watch_index(runtime, source_id) do
    case Map.fetch(runtime.watches, source_id) do
      {:ok, watch} ->
        watch_index = SurfaceIndex.delete(runtime.watch_index, source_id, watch_surface(watch))
        %{runtime | watch_index: watch_index}

      :error ->
        runtime
    end
  end

  defp build_watch_index(watches) do
    watches
    |> Enum.map(fn {source_id, watch} -> {source_id, watch_surface(watch)} end)
    |> SurfaceIndex.rebuild()
  end

  defp watch_surface(%{surface: %InvalidationSurface{} = surface}), do: surface
  defp watch_surface(_watch), do: nil
end
