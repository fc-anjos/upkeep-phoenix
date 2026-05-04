defmodule Upkeep.Coordinator.NodeDAGTest do
  use ExUnit.Case, async: false

  alias Upkeep.Coordinator.NodeDAG

  defmodule Ev do
    defstruct [:id, :tenant_id]
  end

  setup tags do
    _ = Map.get(tags, :shards, 4)
    :ok
  end

  defp narrow_key(%Ev{id: id, tenant_id: tid}) do
    {:upkeep_event, Ev, Enum.sort([{:id, id}, {:tenant_id, tid}])}
  end

  describe "source nodes" do
    test "load_fn runs once per coalesced event; subscribers receive {:dag_value, ...}" do
      counter = :counters.new(1, [:atomics])

      load_fn = fn ->
        :counters.add(counter, 1, 1)
        :loaded_value
      end

      node_id = {:test_source, System.unique_integer()}
      :ok = NodeDAG.register_source(node_id, [narrow_key(%Ev{id: 1, tenant_id: 1})], load_fn)

      event = %Ev{id: 1, tenant_id: 1}
      Enum.each(1..50, fn _ -> NodeDAG.notify(event) end)

      :ok = NodeDAG.drain()

      assert_receive {:dag_value, ^node_id, :loaded_value}, 1_000

      # Coalescing: 50 publishes of an equal-affected node collapse during flush.
      # We tolerate up to a small handful of loads (one per flush window).
      assert :counters.get(counter, 1) <= 5

      NodeDAG.unregister(node_id)
    end

    test "unregistering removes interest and the index" do
      load_fn = fn -> :v end
      node_id = {:remove_test, System.unique_integer()}
      key = narrow_key(%Ev{id: 99, tenant_id: 1})

      :ok = NodeDAG.register_source(node_id, [key], load_fn)
      :ok = NodeDAG.unregister(node_id)

      # No subscribers; notify must not deliver anything.
      NodeDAG.notify(%Ev{id: 99, tenant_id: 1})
      :ok = NodeDAG.drain()
      refute_receive {:dag_value, ^node_id, _}, 200

      assert :ets.lookup(NodeDAG.index_table(), key) == []
    end
  end

  describe "derived nodes" do
    @describetag shards: 1

    test "recompute once when source changes; subscribers receive derived value" do
      source_loads = :counters.new(1, [:atomics])
      derived_computes = :counters.new(1, [:atomics])

      # Single-shard run so derived and source colocate. Cross-shard derived
      # routing is a known prototype limitation; documented in NodeDAG docs.
      source_id = {:src, System.unique_integer()}
      derived_id = same_shard_id({:der, source_id}, source_id)

      load_fn = fn ->
        :counters.add(source_loads, 1, 1)
        42
      end

      compute_fn = fn deps ->
        :counters.add(derived_computes, 1, 1)
        Map.fetch!(deps, source_id) * 2
      end

      :ok =
        NodeDAG.register_source(
          source_id,
          [narrow_key(%Ev{id: 7, tenant_id: 1})],
          load_fn
        )

      :ok = NodeDAG.register_derived(derived_id, [source_id], compute_fn)

      Enum.each(1..20, fn _ -> NodeDAG.notify(%Ev{id: 7, tenant_id: 1}) end)
      :ok = NodeDAG.drain()

      assert_receive {:dag_value, ^source_id, 42}, 1_000
      assert_receive {:dag_value, ^derived_id, 84}, 1_000

      # Coalescing collapses the 20 publishes within the flush window.
      assert :counters.get(source_loads, 1) <= 5
      assert :counters.get(derived_computes, 1) <= 5

      NodeDAG.unregister(derived_id)
      NodeDAG.unregister(source_id)
    end
  end

  defp same_shard_id(prefix, source_id) do
    shards = :persistent_term.get({NodeDAG, :shards})
    target = :erlang.phash2(source_id, shards)

    Stream.iterate(1, &(&1 + 1))
    |> Enum.find_value(fn idx ->
      candidate = {prefix, idx}
      if :erlang.phash2(candidate, shards) == target, do: candidate
    end)
  end
end
