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

  def unregister(source_id) do
    Graph.unregister(source_id)
  end

  def track_source(source_id) do
    Graph.group()
    |> Group.join(Graph.source_key(source_id), %{kind: :local_lv})
    |> case do
      :ok -> :ok
      :already_joined -> :ok
    end
  end

  def untrack_source(source_id), do: unregister(source_id)

  def source_member_count(source_id) do
    source_id
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
