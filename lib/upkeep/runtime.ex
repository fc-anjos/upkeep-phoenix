defmodule Upkeep.Runtime do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Group,
      Phoenix.Component,
      Phoenix.LiveView,
      Phoenix.LiveView.Socket,
      Upkeep.Coordinator,
      Upkeep.DAG,
      Upkeep.Invalidation,
      Upkeep.InvalidationSurface,
      Upkeep.Source,
      Upkeep.Source.Instance,
      Upkeep.Source.Loader,
      Upkeep.Source.LoadResult,
      Upkeep.SingleFlight,
      {Mix, :compile}
    ],
    type: :strict

  alias Upkeep.DAG.Store
  alias Upkeep.Runtime.{Ids, Mount, Patch, Push, Refresh, Result, Snapshot, SourceLoads, Specs}
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.{Telemetry, Watches}

  def mount_source(socket, assign_name, source, params, component, source_location \\ nil) do
    spec = Specs.source(socket, assign_name, source, params, component, source_location)
    announce_registration(spec, source_location)
    mount(socket, spec)
  end

  def mount_component(socket, component_id, deps, fun, source_location \\ nil) do
    spec = Specs.component(socket, component_id, deps, fun, source_location)
    announce_registration(spec, source_location)
    mount(socket, spec)
  end

  def mount_derived(socket, assign_name, deps, fun, source_location \\ nil) do
    spec = Specs.derived(socket, assign_name, deps, fun, source_location)
    announce_registration(spec, source_location)
    mount(socket, spec)
  end

  def sync_current_scope(socket) do
    case Map.fetch(socket.assigns, :current_scope) do
      {:ok, current_scope} ->
        node_id = Ids.scope_node_id(:current_scope)

        if current_scope_synced?(socket, node_id, current_scope) do
          {:ok, socket, []}
        else
          patch =
            socket
            |> Patch.new()
            |> Patch.put_source_node(node_id, current_scope, [], track_change?: true)
            |> Patch.put_assign_node(:current_scope, node_id)
            |> Patch.recompute(&Watches.remove_watch/2)

          Patch.result(patch)
        end

      :error ->
        {:ok, socket, []}
    end
  end

  def mount(socket, spec), do: Mount.dispatch(socket, spec)

  def source_load_coalescer_name, do: SourceLoads.coalescer_name()

  def remove_component(socket, component_id), do: Watches.remove_component(socket, component_id)

  def unwatch_assign(socket, assign_name), do: Watches.unwatch_assign(socket, assign_name)

  def unwatch_source(socket, source, params), do: Watches.unwatch_source(socket, source, params)

  def revoke_authorization(socket, predicate), do: Watches.revoke(socket, predicate)

  def refresh(socket, assign_name, source, params),
    do: Refresh.refresh(socket, assign_name, source, params)

  def refresh_matching(socket, event), do: Refresh.refresh_matching(socket, event)

  def refresh_local_matching(socket, event), do: Refresh.refresh_local_matching(socket, event)

  def queue_matching(socket, event), do: Refresh.queue_matching(socket, event)

  def flush_refreshes(socket), do: Refresh.flush_refreshes(socket)

  def apply_dag_values(socket, pairs), do: Push.apply_dag_values(socket, pairs)

  def apply_dag_value(socket, source_id, value),
    do: Push.apply_dag_value(socket, source_id, value)

  def graph_snapshot(socket), do: Snapshot.build(socket)

  def to_socket(result), do: Result.to_socket(result)

  def recompute_derived(socket, changed_source_nodes, opts \\ []) do
    patch =
      socket
      |> Patch.new()
      |> Patch.mark_changed(changed_source_nodes)
      |> Patch.recompute(&Watches.remove_watch/2, opts)

    {Patch.socket(patch), Patch.effects(patch)}
  end

  defp announce_registration(spec, source_location) do
    Telemetry.emit([:live, :registered], %{}, %{
      node_id: spec.id,
      kind: spec.kind,
      source_location: source_location
    })

    :ok
  end

  defp current_scope_synced?(socket, node_id, current_scope) do
    case Map.fetch(State.assign_nodes(socket), :current_scope) do
      {:ok, ^node_id} ->
        store = State.store(socket)
        Store.has_node?(store, node_id) and Store.fetch!(store, node_id) == current_scope

      _other ->
        false
    end
  end
end
