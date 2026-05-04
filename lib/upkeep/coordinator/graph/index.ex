defmodule Upkeep.Coordinator.Graph.Index do
  @moduledoc false

  alias Upkeep.Coordinator.Graph

  def lookup_nodes(interest_keys) do
    interest_keys
    |> Enum.flat_map(&:ets.lookup(Graph.index_table(), &1))
    |> Enum.map(fn {_key, node_id} -> node_id end)
    |> Enum.uniq()
  end

  def put_source(node_id, shard_idx, interest_keys) do
    :ets.insert(Graph.nodes_table(), {node_id, :source, shard_idx, interest_keys})
    Enum.each(interest_keys, &:ets.insert(Graph.index_table(), {&1, node_id}))
  end

  def put_derived(node_id, shard_idx) do
    :ets.insert(Graph.nodes_table(), {node_id, :derived, shard_idx, []})
  end

  def reconcile_source(node_id, shard_idx, old_keys, new_keys) do
    old_set = MapSet.new(old_keys)
    new_set = MapSet.new(new_keys)

    Enum.each(MapSet.difference(old_set, new_set), fn key ->
      :ets.delete_object(Graph.index_table(), {key, node_id})
    end)

    Enum.each(MapSet.difference(new_set, old_set), fn key ->
      :ets.insert(Graph.index_table(), {key, node_id})
    end)

    :ets.insert(Graph.nodes_table(), {node_id, :source, shard_idx, new_keys})
  end

  def delete(node_id) do
    case lookup(node_id) do
      {:ok, {_kind, _idx, keys}} ->
        Enum.each(keys, &:ets.delete_object(Graph.index_table(), {&1, node_id}))
        :ets.delete(Graph.nodes_table(), node_id)

      :error ->
        :ok
    end
  end

  def lookup(node_id) do
    case :ets.lookup(Graph.nodes_table(), node_id) do
      [{^node_id, kind, shard_idx, keys}] -> {:ok, {kind, shard_idx, keys}}
      _ -> :error
    end
  end

  def owned_nodes(shard_idx) do
    Graph.nodes_table()
    |> :ets.match_object({:_, :_, shard_idx, :_})
  end
end
