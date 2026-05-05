defmodule Upkeep.Coordinator.Graph.Shard.Nodes do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Shard.{Loaders, Retries}
  alias Upkeep.Coordinator.Node
  alias Upkeep.Source.ReadCache
  alias Upkeep.Coordinator.Subscriptions
  alias Upkeep.Coordinator.Topology
  alias Upkeep.DAG.Store

  def register_source(state, node_id, interest_keys, loader) do
    if Store.has_node?(state.store, node_id) do
      state
    else
      encoded_key = Subscriptions.source_key(node_id)
      Topology.register_source(node_id, state.idx, interest_keys)

      {store, _changed?} = Store.put_source(state.store, node_id, nil, [])

      store =
        Store.put_metadata(store, node_id, %Node{
          loader: loader,
          encoded_key: encoded_key,
          registered_keys: interest_keys,
          retry: Loaders.retry_config(loader)
        })

      %{state | store: store}
    end
  end

  def register_derived(state, node_id, dep_ids, compute_fn) do
    encoded_key = Subscriptions.source_key(node_id)
    Topology.register_derived(node_id, state.idx, dep_ids)

    store =
      state.store
      |> Store.register_derived(node_id, dep_ids, compute_fn)
      |> ensure_derived_metadata(node_id, encoded_key)

    %{state | store: store}
  end

  def remove(state, node_id) do
    Topology.unregister(node_id)
    ReadCache.release(node_id)

    state = Retries.clear(state, node_id)

    store =
      if Store.has_node?(state.store, node_id) do
        Store.remove_subgraph(state.store, node_id)
      else
        state.store
      end

    %{state | store: store}
  end

  defp ensure_derived_metadata(store, node_id, encoded_key) do
    case Store.get_metadata(store, node_id) do
      nil -> Store.put_metadata(store, node_id, %Node{encoded_key: encoded_key, loaded?: true})
      _existing -> store
    end
  end
end
