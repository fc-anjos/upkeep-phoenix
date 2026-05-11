defmodule Upkeep.Coordinator.Topology do
  @moduledoc false

  alias Upkeep.InvalidationSurface
  alias Upkeep.Source.Identity, as: SourceIdentity

  @nodes_table :upkeep_topology_nodes
  @index_table :upkeep_topology_index

  ## ETS lifecycle

  def init_tables do
    :ok =
      ensure_named_table!(@nodes_table, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    :ok =
      ensure_named_table!(@index_table, [
        :bag,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])
  end

  def reset do
    :ets.delete_all_objects(@nodes_table)
    :ets.delete_all_objects(@index_table)
    :ok
  end

  ## Mutations

  def register_source(node_id, shard_idx, %InvalidationSurface{} = surface) do
    index_keys = InvalidationSurface.index_keys(surface)

    :ets.insert(@nodes_table, {node_id, shard_idx, %{kind: :source, surface: surface, deps: []}})
    Enum.each(index_keys, &:ets.insert(@index_table, {&1, node_id}))
    :ok
  end

  def register_derived(node_id, shard_idx, deps) do
    :ets.insert(
      @nodes_table,
      {node_id, shard_idx, %{kind: :derived, surface_keys: [], deps: deps}}
    )

    :ok
  end

  def reconcile_source(node_id, shard_idx, old_surface, new_surface) do
    old_set = old_surface |> InvalidationSurface.index_keys() |> MapSet.new()
    new_set = new_surface |> InvalidationSurface.index_keys() |> MapSet.new()

    Enum.each(MapSet.difference(old_set, new_set), fn key ->
      :ets.delete_object(@index_table, {key, node_id})
    end)

    Enum.each(MapSet.difference(new_set, old_set), fn key ->
      :ets.insert(@index_table, {key, node_id})
    end)

    :ets.insert(
      @nodes_table,
      {node_id, shard_idx, %{kind: :source, surface: new_surface, deps: []}}
    )

    :ok
  end

  def unregister(node_id) do
    case lookup(node_id) do
      {:ok, %{kind: :source, surface: surface}} ->
        surface
        |> InvalidationSurface.index_keys()
        |> Enum.each(&:ets.delete_object(@index_table, {&1, node_id}))

        :ets.delete(@nodes_table, node_id)
        :ok

      {:ok, %{kind: :derived}} ->
        :ets.delete(@nodes_table, node_id)
        :ok

      :error ->
        :ok
    end
  end

  ## Queries

  def lookup(node_id) do
    case :ets.lookup(@nodes_table, node_id) do
      [{^node_id, shard_idx, %{kind: :source, surface: surface} = node}] ->
        {:ok,
         node
         |> Map.put(:shard_idx, shard_idx)
         |> Map.put(:surface_keys, InvalidationSurface.index_keys(surface))}

      [{^node_id, shard_idx, node}] ->
        {:ok, Map.put(node, :shard_idx, shard_idx)}

      _ ->
        :error
    end
  end

  def registered?(node_id) do
    match?({:ok, _}, lookup(node_id))
  end

  def affected_source_node_ids(event) when is_struct(event) do
    candidate_keys = InvalidationSurface.candidate_keys(event)

    candidate_node_ids =
      candidate_keys
      |> Enum.flat_map(&:ets.lookup(@index_table, &1))
      |> Enum.map(fn {_key, node_id} -> node_id end)
      |> Enum.uniq()

    matched_node_ids = Enum.filter(candidate_node_ids, &source_node_matches?(&1, event))

    emit_invalidation(event, candidate_keys, candidate_node_ids, matched_node_ids)

    matched_node_ids
  end

  def affected_source_node_ids(event, shard_idx)
      when is_struct(event) and is_integer(shard_idx) do
    event
    |> affected_source_node_ids()
    |> Enum.filter(&(shard_of_node(&1) == shard_idx))
  end

  def owned_nodes(shard_idx) do
    @nodes_table
    |> :ets.match_object({:_, shard_idx, :_})
    |> Enum.map(fn {node_id, _idx, _node} -> node_id end)
  end

  ## Sharding

  def shard_count, do: :persistent_term.get({__MODULE__, :shards})

  def put_shard_count(count) when is_integer(count) and count > 0 do
    :persistent_term.put({__MODULE__, :shards}, count)
  end

  def shard_of(node_id) do
    :erlang.phash2(node_partition(node_id), shard_count())
  end

  def shard_of_node(node_id) do
    case lookup(node_id) do
      {:ok, %{shard_idx: shard_idx}} -> shard_idx
      :error -> shard_of(node_id)
    end
  end

  def node_partition({source, params}) when is_atom(source) and is_map(params) do
    SourceIdentity.sharing_partition(source, params)
  end

  def node_partition({:derived, _view, _assign_name, dep_node_ids, _fun}) do
    case shared_partition(dep_node_ids) do
      {:ok, partition} -> partition
      {:error, reason} -> {:derived, reason, dep_node_ids}
    end
  end

  def node_partition(node_id), do: {:node, node_id}

  def shared_partition_info(node_ids) do
    dep_partitions = Enum.map(node_ids, &{&1, node_partition(&1)})

    case dep_partitions |> Enum.map(&elem(&1, 1)) |> Enum.uniq() do
      [partition] -> {:ok, partition, dep_partitions}
      [] -> {:error, :empty_deps, dep_partitions}
      _partitions -> {:error, :cross_partition_dep, dep_partitions}
    end
  end

  def shared_partition(node_ids) do
    case shared_partition_info(node_ids) do
      {:ok, partition, _dep_partitions} -> {:ok, partition}
      {:error, reason, _dep_partitions} -> {:error, reason}
    end
  end

  def dependency_shard_groups(dep_node_ids) when is_list(dep_node_ids) do
    dep_node_ids
    |> Enum.group_by(&shard_of_node/1)
    |> Enum.map(fn {shard, node_ids} ->
      %{shard: shard, node_ids: node_ids, count: length(node_ids)}
    end)
    |> Enum.sort_by(&{-&1.count, &1.shard})
  end

  defp ensure_named_table!(name, opts) do
    case :ets.whereis(name) do
      :undefined ->
        ^name = :ets.new(name, opts)
        :ok

      _ ->
        :ok
    end
  end

  defp source_node_matches?(node_id, event) do
    case lookup(node_id) do
      {:ok, %{kind: :source, surface: surface}} -> InvalidationSurface.matches?(surface, event)
      _ -> false
    end
  end

  defp emit_invalidation(event, candidate_keys, candidate_node_ids, matched_node_ids) do
    :telemetry.execute(
      [:upkeep, :graph, :invalidation],
      %{
        count: 1,
        candidate_key_count: length(candidate_keys),
        candidate_count: length(candidate_node_ids),
        matched_count: length(matched_node_ids)
      },
      InvalidationSurface.event_metadata(event)
    )
  end
end
