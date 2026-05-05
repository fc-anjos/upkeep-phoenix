defmodule Upkeep.Coordinator.Graph.Shard.Lifecycle do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Shard.Nodes
  alias Upkeep.Coordinator.Subscriptions
  alias Upkeep.Coordinator.Topology

  @generation_table :upkeep_graph_shard_generations

  def start(idx) do
    sweep_owned_ets(idx)

    :ok = Subscriptions.monitor_sources()

    generation = bump_generation(idx)

    :ok = Subscriptions.join_shard(idx, generation)

    generation
  end

  def handle_group_events(events, state) do
    Enum.reduce(events, state, &handle_group_event/2)
  end

  defp handle_group_event(%{type: :left, key: key}, state) do
    case decode_owned_source_key(key, state.idx) do
      :other ->
        state

      node_id ->
        if Subscriptions.member_count(key) == 0,
          do: Nodes.remove(state, node_id),
          else: state
    end
  end

  defp handle_group_event(_other, state), do: state

  defp decode_owned_source_key(key, idx) do
    node_id = Subscriptions.decode_source_key(key)

    case Topology.lookup(node_id) do
      {:ok, %{shard_idx: ^idx}} -> node_id
      _ -> :other
    end
  rescue
    _ -> :other
  end

  defp sweep_owned_ets(idx) do
    idx
    |> Topology.owned_nodes()
    |> Enum.each(&Topology.unregister/1)
  end

  defp bump_generation(idx) do
    case :ets.info(@generation_table) do
      :undefined ->
        :ets.new(@generation_table, [:set, :public, :named_table, write_concurrency: true])

      _ ->
        :ok
    end

    :ets.update_counter(@generation_table, idx, 1, {idx, 0})
  end
end
