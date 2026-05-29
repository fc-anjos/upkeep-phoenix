defmodule Upkeep.Runtime.Subscriptions do
  @moduledoc false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.InvalidationSurface
  alias Upkeep.Source.Instance

  def register(source_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    :ok = Graph.register_source(source_id, surface, instance)
  end

  def register_and_load(source_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    Graph.register_source_and_load(source_id, surface, instance)
  end

  def register_derived_and_compute(graph_node_id, dep_node_ids, dep_values, compute, metadata) do
    Graph.register_derived_and_compute(graph_node_id, dep_node_ids, dep_values, compute, metadata)
  end

  def unregister(source_id) do
    Graph.unregister(source_id)
  end

  def track_source(source_id, identity \\ nil) do
    Graph.group()
    |> Group.join(Graph.source_key(source_id), %{kind: :local_lv, authorizing_identity: identity})
    |> case do
      :ok -> :ok
      :already_joined -> :ok
    end
  end

  # The identity that authorized a watch, captured from the viewer scope at watch
  # time. A configurable extractor maps the scope to whatever the application
  # treats as the authorization principal (defaults to the scope itself).
  def authorizing_identity(%Phoenix.LiveView.Socket{} = socket) do
    extractor = Application.get_env(:upkeep, :authorizing_identity, & &1)
    extractor.(Map.get(socket.assigns, :current_scope))
  end

  def untrack_source(source_id), do: unregister(source_id)

  def source_member_count(source_id) do
    source_id
    |> Graph.source_key()
    |> Graph.member_count()
  end

  def node_member_count(node_id) do
    node_id
    |> Graph.source_key()
    |> Graph.member_count()
  end

  def registered_source?(source_id), do: Graph.registered?(source_id)

  def join_local_notifications do
    Upkeep.Invalidation.join_notifications(:live_local)
  end

  def leave_local_notifications do
    Upkeep.Invalidation.leave_notifications()
  end

  def register_interest?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: true

  def register_interest?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)

  def shared_initial_load?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: false

  def shared_initial_load?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)

  def tracking_policy(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: :eager

  def tracking_policy(%Phoenix.LiveView.Socket{} = socket) do
    if Phoenix.LiveView.connected?(socket), do: :auto, else: :local
  end
end
