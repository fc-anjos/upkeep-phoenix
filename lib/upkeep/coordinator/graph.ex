defmodule Upkeep.Coordinator.Graph do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Notifier
  alias Upkeep.Coordinator.DerivedProcesses
  alias Upkeep.Coordinator.SourceProcesses
  alias Upkeep.Coordinator.Subscriptions
  alias Upkeep.Coordinator.Topology
  alias Upkeep.InvalidationSurface
  alias Upkeep.Source.Instance

  ## Public API

  def start_link(opts \\ []) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    Upkeep.Coordinator.Supervisor.start_link(opts)
  end

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  defdelegate group, to: Subscriptions

  def register_source(node_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    :ok = SourceProcesses.register_source(node_id, surface, instance)
    subscribe_source(node_id)
  end

  def register_source_and_load(node_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    {:ok, result} = SourceProcesses.register_source_and_load(node_id, surface, instance)
    subscribe_source(node_id)
    {:ok, result}
  end

  def register_derived_and_compute(node_id, dep_node_ids, dep_values, compute, metadata)
      when is_list(dep_node_ids) and is_map(dep_values) and is_function(compute, 1) do
    :ok = Subscriptions.subscribe(node_id)

    case DerivedProcesses.register_and_compute(
           node_id,
           dep_node_ids,
           dep_values,
           compute,
           metadata
         ) do
      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        :ok = Subscriptions.unsubscribe(node_id)
        {:error, reason}
    end
  end

  def register_loader(node_id, %InvalidationSurface{} = surface, load_fn)
      when is_function(load_fn, 0) do
    :ok = SourceProcesses.register_loader(node_id, surface, load_fn)
    subscribe_source(node_id)
  end

  def unregister(node_id) do
    :ok = Subscriptions.unsubscribe(node_id)

    if Subscriptions.member_count(Subscriptions.source_key(node_id)) == 0 do
      :ok = SourceProcesses.release(node_id)
      DerivedProcesses.release(node_id)
    else
      :ok
    end
  end

  defdelegate subscribers(node_id), to: Subscriptions
  defdelegate subscribed?(node_id, pid \\ nil), to: Subscriptions
  defdelegate member_count(encoded_key), to: Subscriptions
  defdelegate registered?(node_id), to: Topology

  def drain do
    Notifier.drain()
    SourceProcesses.drain_all()
    DerivedProcesses.drain_all()
  end

  def reset do
    DerivedProcesses.reset_all()
    SourceProcesses.reset_all()
    Topology.reset()

    :ok
  end

  defdelegate task_sup, to: SourceProcesses
  defdelegate source_key(node_id), to: Subscriptions

  defdelegate decode_source_key(key), to: Subscriptions

  defp subscribe_source(node_id) do
    :ok = Subscriptions.subscribe(node_id)
    SourceProcesses.touch_subscribers(node_id)
    :ok
  end
end
