defmodule Upkeep.Coordinator.Graph do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Notifier
  alias Upkeep.Coordinator.Shards
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
    :ok = Shards.register_source(node_id, surface, instance)
    Subscriptions.subscribe(node_id)
  end

  def register_source_and_load(node_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    {:ok, result} = Shards.register_source_and_load(node_id, surface, instance)
    Subscriptions.subscribe(node_id)
    {:ok, result}
  end

  def register_loader(node_id, %InvalidationSurface{} = surface, load_fn)
      when is_function(load_fn, 0) do
    :ok = Shards.register_loader(node_id, surface, load_fn)
    Subscriptions.subscribe(node_id)
  end

  def register_derived(node_id, dep_node_ids, compute_fn)
      when is_list(dep_node_ids) and is_function(compute_fn, 1) do
    :ok = Shards.register_derived(node_id, dep_node_ids, compute_fn)
    Subscriptions.subscribe(node_id)
  end

  def register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata \\ %{}) do
    Shards.register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata)
  end

  defdelegate unregister(node_id), to: Subscriptions, as: :unsubscribe
  defdelegate subscribers(node_id), to: Subscriptions
  defdelegate subscribed?(node_id, pid \\ nil), to: Subscriptions
  defdelegate member_count(encoded_key), to: Subscriptions
  defdelegate registered?(node_id), to: Topology

  def drain do
    Notifier.drain()
    Shards.drain_all()
  end

  def reset do
    Shards.reset_all()
    Topology.reset()

    :ok
  end

  defdelegate shared_partition_info(node_ids), to: Topology

  defdelegate shard_name(idx), to: Shards, as: :name
  defdelegate task_sup, to: Shards
  defdelegate shard_count, to: Shards
  defdelegate source_key(node_id), to: Subscriptions

  defdelegate decode_source_key(key), to: Subscriptions
  defdelegate shard_key(idx), to: Subscriptions
end
