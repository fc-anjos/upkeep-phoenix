defmodule Upkeep.Coordinator.Topology do
  @moduledoc false

  alias Upkeep.Source.Identity, as: SourceIdentity
  alias Upkeep.Source.Reactivity, as: SourceReactivity

  @nodes_table :upkeep_topology_nodes
  @index_table :upkeep_topology_index

  ## ETS lifecycle

  def init_tables do
    ensure_table(@nodes_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    ensure_table(@index_table, [
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

  def register_source(node_id, shard_idx, interest_keys) do
    :ets.insert(@nodes_table, {node_id, :source, shard_idx, interest_keys, []})
    Enum.each(interest_keys, &:ets.insert(@index_table, {&1, node_id}))
    :ok
  end

  def register_derived(node_id, shard_idx, deps) do
    :ets.insert(@nodes_table, {node_id, :derived, shard_idx, [], deps})
    :ok
  end

  def reconcile_source(node_id, shard_idx, old_keys, new_keys) do
    old_set = MapSet.new(old_keys)
    new_set = MapSet.new(new_keys)

    Enum.each(MapSet.difference(old_set, new_set), fn key ->
      :ets.delete_object(@index_table, {key, node_id})
    end)

    Enum.each(MapSet.difference(new_set, old_set), fn key ->
      :ets.insert(@index_table, {key, node_id})
    end)

    :ets.insert(@nodes_table, {node_id, :source, shard_idx, new_keys, []})
    :ok
  end

  def unregister(node_id) do
    case lookup(node_id) do
      {:ok, %{interest_keys: keys}} ->
        Enum.each(keys, &:ets.delete_object(@index_table, {&1, node_id}))
        :ets.delete(@nodes_table, node_id)
        :ok

      :error ->
        :ok
    end
  end

  ## Queries

  def lookup(node_id) do
    case :ets.lookup(@nodes_table, node_id) do
      [{^node_id, kind, shard_idx, interest_keys, deps}] ->
        {:ok, %{kind: kind, shard_idx: shard_idx, interest_keys: interest_keys, deps: deps}}

      _ ->
        :error
    end
  end

  def affected_source_node_ids(event) when is_struct(event) do
    affected =
      event
      |> SourceReactivity.event_keys()
      |> Enum.flat_map(&:ets.lookup(@index_table, &1))
      |> Enum.map(fn {_key, node_id} -> node_id end)

    affected =
      if Upkeep.Change.broad_update?(event) do
        affected ++ broad_update_source_node_ids(event)
      else
        affected
      end

    Enum.uniq(affected)
  end

  def affected_source_node_ids(event, shard_idx)
      when is_struct(event) and is_integer(shard_idx) do
    event
    |> affected_source_node_ids()
    |> Enum.filter(&(shard_of_node(&1) == shard_idx))
  end

  def owned_nodes(shard_idx) do
    @nodes_table
    |> :ets.match_object({:_, :_, shard_idx, :_, :_})
    |> Enum.map(fn {node_id, _kind, _idx, _keys, _deps} -> node_id end)
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

  defp ensure_table(name, opts) do
    case :ets.info(name) do
      :undefined -> :ets.new(name, opts)
      _ -> :ok
    end
  end

  defp broad_update_source_node_ids(%Upkeep.Change{} = change) do
    @nodes_table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {node_id, :source, _shard_idx, interest_keys, _deps} ->
        if Enum.any?(interest_keys, &updated_interest_key?(&1, change.schema)) do
          [node_id]
        else
          []
        end

      _other ->
        []
    end)
  end

  defp updated_interest_key?({:upkeep_change, :updated, key_schema}, schema) do
    change_schema_matches?(schema, key_schema)
  end

  defp updated_interest_key?({:upkeep_change, :updated, key_schema, _values}, schema) do
    change_schema_matches?(schema, key_schema)
  end

  defp updated_interest_key?(_key, _schema), do: false

  defp change_schema_matches?(_schema, :_), do: true
  defp change_schema_matches?(schema, schema), do: true
  defp change_schema_matches?(_schema, _key_schema), do: false
end
