defmodule Upkeep.Runtime do
  @moduledoc false

  alias Upkeep.Internal.DAG.Store
  alias Upkeep.Live.Ids
  alias Upkeep.Runtime.DAGOperations
  alias Upkeep.Runtime.Mount
  alias Upkeep.Runtime.Push
  alias Upkeep.Runtime.Refresh
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Watches

  def sync_current_scope(socket) do
    case Map.fetch(socket.assigns, :current_scope) do
      {:ok, current_scope} ->
        node_id = Ids.scope_node_id(:current_scope)

        {store, changed?} =
          socket
          |> State.store()
          |> Store.put_source(node_id, current_scope, [])

        socket =
          socket
          |> State.put_store(store)
          |> State.put_assign_node(:current_scope, node_id)

        if changed? do
          {socket, effects} = recompute_derived(socket, [node_id])
          {:ok, socket, effects}
        else
          {:ok, socket, []}
        end

      :error ->
        {:ok, socket, []}
    end
  end

  def mount(socket, spec), do: Mount.dispatch(socket, spec)

  def remove_component(socket, component_id), do: Watches.remove_component(socket, component_id)

  def unwatch_assign(socket, assign_name), do: Watches.unwatch_assign(socket, assign_name)

  def unwatch_source(socket, source, params), do: Watches.unwatch_source(socket, source, params)

  def refresh(socket, assign_name, source, params),
    do: Refresh.refresh(socket, assign_name, source, params)

  def refresh_matching(socket, event), do: Refresh.refresh_matching(socket, event)

  def queue_matching(socket, event), do: Refresh.queue_matching(socket, event)

  def flush_refreshes(socket), do: Refresh.flush_refreshes(socket)

  def apply_dag_values(socket, pairs), do: Push.apply_dag_values(socket, pairs)

  def apply_dag_value(socket, source_id, value),
    do: Push.apply_dag_value(socket, source_id, value)

  def recompute_derived(socket, changed_source_nodes, opts \\ []) do
    DAGOperations.recompute_derived(socket, changed_source_nodes, &Watches.remove_watch/2, opts)
  end
end
