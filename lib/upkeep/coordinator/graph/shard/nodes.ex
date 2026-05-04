defmodule Upkeep.Coordinator.Graph.Shard.Nodes do
  @moduledoc false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Index
  alias Upkeep.Coordinator.Graph.Shard.Retries
  alias Upkeep.Coordinator.Node
  alias Upkeep.Coordinator.ReadNodes
  alias Upkeep.DAG

  def register_source(state, node_id, interest_keys, loader) do
    case Map.fetch(state.sources, node_id) do
      {:ok, %Node{}} ->
        state

      :error ->
        encoded_key = Graph.source_key(node_id)
        Index.put_source(node_id, state.idx, interest_keys)
        {dag, _changed?} = DAG.put_source(state.dag, node_id, nil, [])

        %{
          state
          | sources:
              Map.put(state.sources, node_id, %Node{
                id: node_id,
                kind: :source,
                loader: loader,
                registered_keys: interest_keys,
                encoded_key: encoded_key
              }),
            dag: dag
        }
    end
  end

  def register_derived(state, node_id, dep_ids, compute_fn) do
    encoded_key = Graph.source_key(node_id)
    Index.put_derived(node_id, state.idx)

    %{
      state
      | dag: DAG.put_derived(state.dag, node_id, dep_ids, compute_fn, initial_value: nil),
        sources:
          Map.put_new(state.sources, node_id, %Node{
            id: node_id,
            kind: :derived,
            encoded_key: encoded_key,
            loaded?: true
          })
    }
  end

  def remove(state, node_id) do
    Index.delete(node_id)
    ReadNodes.release(node_id)

    state = Retries.clear(state, node_id)

    %{
      state
      | sources: Map.delete(state.sources, node_id),
        dag:
          if(DAG.has_node?(state.dag, node_id),
            do: DAG.remove_subgraph(state.dag, node_id),
            else: state.dag
          )
    }
  end
end
