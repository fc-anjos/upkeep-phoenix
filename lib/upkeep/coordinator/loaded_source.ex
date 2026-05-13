defmodule Upkeep.Coordinator.LoadedSource do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Shard.Loaders
  alias Upkeep.Coordinator.{Node, Subscriptions, Topology}
  alias Upkeep.DAG.Store
  alias Upkeep.Source.LoadResult

  defstruct [
    :node_id,
    :node,
    :generation,
    :source_result,
    :value,
    :surface,
    surface_keys: [],
    tracked_deps: []
  ]

  def from_source_result(node_id, %Node{} = node, %LoadResult{} = result) do
    %__MODULE__{
      node_id: node_id,
      node: node,
      source_result: result,
      value: result.value,
      surface_keys: Upkeep.InvalidationSurface.keys(result.surface),
      tracked_deps: result.tracked_deps,
      surface: result.surface
    }
  end

  def from_fun(node_id, %Node{} = node, value, surface) do
    %__MODULE__{
      node_id: node_id,
      node: node,
      source_result: nil,
      value: value,
      surface_keys: Upkeep.InvalidationSurface.index_keys(surface),
      tracked_deps: [],
      surface: surface
    }
  end

  def with_generation(%__MODULE__{} = loaded, generation) when is_integer(generation) do
    %{loaded | generation: generation}
  end

  def apply_if_current(state, %__MODULE__{} = loaded) do
    if current_generation?(loaded) do
      {:applied, apply_loaded(state, loaded)}
    else
      {:stale, state}
    end
  end

  def apply(state, %__MODULE__{} = loaded) do
    apply_loaded(state, loaded)
  end

  defp apply_loaded(state, %__MODULE__{node_id: node_id, node: %Node{} = node} = loaded) do
    state =
      if loaded.surface != node.surface do
        Topology.reconcile_source(node_id, state.idx, node.surface, loaded.surface)
        state
      else
        state
      end

    {store, _changed?} = Store.put_source(state.store, node_id, loaded.value, [])

    store =
      Store.put_metadata(
        store,
        node_id,
        %Node{
          node
          | surface_keys: loaded.surface_keys,
            surface: loaded.surface,
            tracked_deps: loaded.tracked_deps,
            loaded?: true
        }
      )

    %{state | store: store}
  end

  def pair(%__MODULE__{node_id: node_id, value: value}), do: {node_id, value}

  def reply(%__MODULE__{source_result: source_result}), do: {:ok, source_result}

  def load_metadata(state, node_id, %Node{} = node, load_reason) do
    node.loader
    |> Loaders.metadata()
    |> Map.put(:shard, state.idx)
    |> Map.put(:node_id, node_id)
    |> Map.put(:load_reason, load_reason)
    |> Map.put(:subscriber_count, subscriber_count(node))
  end

  defp subscriber_count(%Node{encoded_key: encoded_key}) do
    Subscriptions.member_count(encoded_key)
  end

  defp current_generation?(%__MODULE__{generation: nil}), do: true

  defp current_generation?(%__MODULE__{node_id: node_id, generation: generation}) do
    Topology.generation(node_id) == generation
  end
end
