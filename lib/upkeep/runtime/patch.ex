defmodule Upkeep.Runtime.Patch do
  @moduledoc false

  alias Upkeep.DAG.{Graph, Store}
  alias Upkeep.Live.{Components, Ids, Telemetry}
  alias Upkeep.Runtime.{Effects, State}

  defstruct socket: nil,
            changed_nodes: [],
            shared_nodes: [],
            removed_nodes: [],
            ignored: [],
            effects: [],
            recompute_effects: []

  def new(socket), do: %__MODULE__{socket: socket}

  def socket(%__MODULE__{socket: socket}), do: socket
  def changed_nodes(%__MODULE__{changed_nodes: changed_nodes}), do: changed_nodes
  def shared_nodes(%__MODULE__{shared_nodes: shared_nodes}), do: shared_nodes
  def ignored(%__MODULE__{ignored: ignored}), do: ignored
  def effects(%__MODULE__{effects: effects}), do: effects
  def recompute_effects(%__MODULE__{recompute_effects: recompute_effects}), do: recompute_effects

  def result(%__MODULE__{} = patch), do: {:ok, patch.socket, patch.effects}

  def replace_socket(%__MODULE__{} = patch, socket), do: %{patch | socket: socket}

  def append_effects(%__MODULE__{} = patch, effects) when is_list(effects) do
    %{patch | effects: patch.effects ++ effects}
  end

  def put_assign_node(%__MODULE__{} = patch, assign_name, node_id) when is_atom(assign_name) do
    replace_socket(patch, State.put_assign_node(patch.socket, assign_name, node_id))
  end

  def put_derive_sharing(%__MODULE__{} = patch, node_id, metadata) when is_map(metadata) do
    replace_socket(patch, State.put_derive_sharing(patch.socket, node_id, metadata))
  end

  def put_shared_derived_node(%__MODULE__{} = patch, _node_id, nil), do: patch

  def put_shared_derived_node(%__MODULE__{} = patch, node_id, graph_node_id) do
    replace_socket(patch, State.put_shared_derived_node(patch.socket, node_id, graph_node_id))
  end

  def mark_changed(%__MODULE__{} = patch, node_ids) when is_list(node_ids) do
    %{patch | changed_nodes: patch.changed_nodes ++ node_ids}
  end

  def put_source(%__MODULE__{} = patch, source_id, value, deps, opts \\ []) do
    put_source_node(patch, Ids.source_node_id(source_id), value, deps, opts)
  end

  def put_source_node(%__MODULE__{} = patch, node_id, value, deps, opts \\ [])
      when is_list(deps) do
    {store, changed?} =
      patch.socket
      |> State.store()
      |> Store.put_source(node_id, value, deps)

    store = put_runtime_metadata(store, node_id, Keyword.get(opts, :metadata))
    patch = replace_socket(patch, State.put_store(patch.socket, store))

    if Keyword.get(opts, :track_change?, false) and changed? do
      %{patch | changed_nodes: patch.changed_nodes ++ [node_id]}
    else
      patch
    end
  end

  def put_watch_value(%__MODULE__{} = patch, watch, value) do
    patch
    |> put_source(watch.source_id, value, Ids.source_deps(watch.component), track_change?: true)
    |> append_effects(Effects.assign_watch(watch, value))
  end

  def put_component(%__MODULE__{} = patch, node_id, deps, compute, metadata, component_id)
      when is_list(deps) and is_function(compute, 1) do
    store =
      patch.socket
      |> State.store()
      |> Store.put_component(node_id, deps, compute)
      |> put_runtime_metadata(node_id, metadata)

    value = Store.fetch!(store, node_id)
    store = Components.put_assign_nodes(store, component_id, value)

    socket =
      patch.socket
      |> State.put_store(store)
      |> put_component_assign_nodes(component_id, value)

    patch
    |> replace_socket(socket)
    |> append_effects(Effects.component_assigns(value))
  end

  def register_derived(%__MODULE__{} = patch, node_id, deps, compute, initial_value, metadata)
      when is_list(deps) and is_function(compute, 1) do
    store =
      patch.socket
      |> State.store()
      |> Store.register_derived(node_id, deps, compute)
      |> put_runtime_metadata(node_id, metadata)
      |> seed_value(node_id, initial_value)

    replace_socket(patch, State.put_store(patch.socket, store))
  end

  def put_shared_derived_value(%__MODULE__{} = patch, local_node_id, value) do
    {store, changed?} =
      patch.socket
      |> State.store()
      |> Store.seed(local_node_id, value)

    socket = State.put_store(patch.socket, store)

    patch =
      patch
      |> replace_socket(socket)
      |> append_effects(Effects.assign_shared_derived(socket, local_node_id, value))

    patch = %{patch | shared_nodes: patch.shared_nodes ++ [local_node_id]}

    if changed? do
      %{patch | changed_nodes: patch.changed_nodes ++ [local_node_id]}
    else
      patch
    end
  end

  def ignore(%__MODULE__{} = patch, reason, node_id) do
    %{patch | ignored: patch.ignored ++ [%{reason: reason, node_id: node_id}]}
  end

  def recompute(patch, remove_watch, opts \\ [])

  def recompute(%__MODULE__{changed_nodes: []} = patch, _remove_watch, _opts), do: patch

  def recompute(%__MODULE__{} = patch, remove_watch, opts)
      when is_function(remove_watch, 2) do
    skip_node_ids = Keyword.get(opts, :skip, patch.shared_nodes)

    {store, diff} =
      Telemetry.span([:dag, :recompute], %{changed_source_nodes: patch.changed_nodes}, fn ->
        patch.socket
        |> State.store()
        |> Store.recompute(patch.changed_nodes, skip: skip_node_ids)
        |> then(fn {_store, diff} = result ->
          {result,
           %{
             affected_nodes: diff.selected_node_ids,
             changed_derived_nodes: diff.changed_node_ids,
             recomputed_nodes: diff.recomputed_node_ids,
             changed_count: length(diff.changed_node_ids),
             recomputed_count: length(diff.recomputed_node_ids),
             skipped_nodes: diff.skipped_node_ids
           }}
        end)
      end)

    patch =
      patch
      |> replace_socket(State.put_store(patch.socket, store))
      |> remove_changed_component_watches(diff.changed_node_ids, remove_watch)

    {socket, assign_effects} = assign_derived_nodes(patch.socket, diff.changed_node_ids)
    recompute_effects = patch.recompute_effects ++ assign_effects

    %{
      patch
      | socket: socket,
        effects: patch.effects ++ assign_effects,
        recompute_effects: recompute_effects
    }
  end

  def subgraph_node_ids(socket, root_id) do
    socket
    |> State.store()
    |> Store.graph()
    |> Graph.subgraph_plan(root_id)
    |> Map.fetch!(:selected_node_ids)
  end

  def remove_subgraph(%__MODULE__{} = patch, root_id) do
    removed_node_ids = subgraph_node_ids(patch.socket, root_id)

    store =
      patch.socket
      |> State.store()
      |> Store.remove_subgraph(root_id)

    assign_names =
      Enum.flat_map(removed_node_ids, &State.assign_names_for_node(patch.socket, &1))

    socket =
      Enum.reduce(assign_names, State.put_store(patch.socket, store), fn assign_name, socket ->
        State.delete_assign_node(socket, assign_name)
      end)

    %{patch | socket: socket, removed_nodes: patch.removed_nodes ++ removed_node_ids}
  end

  def put_runtime_metadata(store, _node_id, nil), do: store
  def put_runtime_metadata(store, _node_id, metadata) when metadata == %{}, do: store

  def put_runtime_metadata(store, node_id, metadata) when is_map(metadata) do
    Store.put_metadata(store, node_id, metadata)
  end

  defp remove_changed_component_watches(%__MODULE__{} = patch, node_ids, remove_watch) do
    node_ids
    |> Components.changed_component_ids()
    |> Enum.reduce(patch, fn component_id, patch ->
      patch.socket
      |> State.watches()
      |> Enum.filter(fn {_source_id, watch} -> watch.component == component_id end)
      |> Enum.reduce(patch, fn {source_id, _watch}, patch ->
        {socket, remove_effects} = remove_watch.(patch.socket, source_id)

        patch
        |> replace_socket(socket)
        |> append_effects(remove_effects)
        |> put_recompute_effects(remove_effects)
      end)
    end)
  end

  defp assign_derived_nodes(socket, node_ids) do
    Enum.reduce(node_ids, {socket, []}, fn node_id, {socket, effects} ->
      value = Store.fetch!(State.store(socket), node_id)
      {socket, effects ++ assign_node_effects(socket, node_id, value)}
    end)
  end

  defp assign_node_effects(_socket, {:component, _component_id}, value) when is_map(value) do
    Effects.component_assigns(value)
  end

  defp assign_node_effects(_socket, {:component, _component_id}, _value), do: []

  defp assign_node_effects(socket, node_id, value) do
    socket
    |> State.assign_names_for_node(node_id)
    |> Enum.flat_map(fn assign_name ->
      Effects.assign_derived(assign_name, value, node_id)
    end)
  end

  defp put_recompute_effects(%__MODULE__{} = patch, effects) do
    %{patch | recompute_effects: patch.recompute_effects ++ effects}
  end

  defp put_component_assign_nodes(socket, component_id, value) when is_map(value) do
    Enum.reduce(value, socket, fn
      {assign_name, _assign_value}, socket when is_atom(assign_name) ->
        State.put_assign_node(
          socket,
          assign_name,
          Ids.component_assign_node_id(component_id, assign_name)
        )

      {_assign_name, _assign_value}, socket ->
        socket
    end)
  end

  defp put_component_assign_nodes(socket, _component_id, _value), do: socket

  defp seed_value(store, id, value) do
    {store, _changed?} = Store.seed(store, id, value)
    store
  end
end
