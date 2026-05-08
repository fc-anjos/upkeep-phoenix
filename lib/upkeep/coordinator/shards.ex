defmodule Upkeep.Coordinator.Shards do
  @moduledoc false

  alias Upkeep.Coordinator.Topology
  alias Upkeep.InvalidationSurface
  alias Upkeep.Source.Instance

  @task_sup Upkeep.Coordinator.Graph.TaskSup

  def task_sup, do: @task_sup
  def name(idx), do: :"Elixir.Upkeep.Coordinator.Graph.Shard.#{idx}"

  def register_source(node_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    call_owner(node_id, {:register_source, node_id, surface, {:source, instance}})
  end

  def register_source_and_load(node_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    call_owner(
      node_id,
      {:register_source_and_load, node_id, surface, {:source, instance}},
      30_000
    )
  end

  def register_loader(node_id, %InvalidationSurface{} = surface, load_fn)
      when is_function(load_fn, 0) do
    call_owner(node_id, {:register_source, node_id, surface, {:fun, load_fn}})
  end

  def register_derived(node_id, dep_node_ids, compute_fn)
      when is_list(dep_node_ids) and is_function(compute_fn, 1) do
    shard = derived_shard!(node_id, dep_node_ids)
    GenServer.call(name(shard), {:register_derived, node_id, dep_node_ids, compute_fn})
  end

  def register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata)
      when is_list(dep_node_ids) and is_map(dep_values) and is_function(compute_fn, 1) and
             is_map(metadata) do
    call_owner(
      node_id,
      {:register_derived_and_compute, node_id, dep_node_ids, dep_values, compute_fn, metadata},
      30_000
    )
  end

  def notify_source_nodes(shard, node_ids) when is_integer(shard) and is_list(node_ids) do
    GenServer.cast(name(shard), {:notify_source_nodes, node_ids})
  end

  def drain_all do
    for_each_shard(&GenServer.call(name(&1), :drain, 60_000))
    :ok
  end

  def reset_all do
    for_each_shard(&GenServer.call(name(&1), :reset, 60_000))
    :ok
  end

  def shard_count, do: Topology.shard_count()

  defp call_owner(node_id, message, timeout \\ 5_000) do
    node_id
    |> Topology.shard_of()
    |> name()
    |> GenServer.call(message, timeout)
  end

  defp for_each_shard(fun) do
    Enum.each(0..(Topology.shard_count() - 1), fun)
  end

  defp derived_shard!(node_id, dep_node_ids) do
    groups = Topology.dependency_shard_groups(dep_node_ids)

    case groups do
      [%{shard: shard}] ->
        shard

      [] ->
        raise ArgumentError,
              "register_derived/3 for #{inspect(node_id)} requires at least one dep"

      [_ | _] = multi ->
        shards = Enum.map(multi, & &1.shard)

        raise ArgumentError,
              "register_derived/3 for #{inspect(node_id)} has deps split across shards " <>
                "#{inspect(shards)}. #{cross_shard_message(multi)} " <>
                "cross-shard recompute is not implemented yet."
    end
  end

  defp cross_shard_message(groups) do
    largest_count = groups |> Enum.map(& &1.count) |> Enum.max()
    largest = Enum.filter(groups, &(&1.count == largest_count))

    "The largest colocated dependency group is " <>
      Enum.map_join(largest, "; ", fn group ->
        "shard #{group.shard} with #{group.count} dep(s): #{inspect(group.node_ids)}"
      end) <>
      ". Reshape the derived node around the largest group, or keep this compute local. "
  end
end
