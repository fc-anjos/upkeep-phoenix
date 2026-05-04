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
      attach_telemetry([
        [:upkeep, :node_dag, :dispatch, :start],
        [:upkeep, :node_dag, :dispatch, :stop]
      ])

      counter = :counters.new(1, [:atomics])

      keys = [narrow_key(%Ev{id: 1, tenant_id: 1})]

      load_fn = fn ->
        :counters.add(counter, 1, 1)
        {:loaded_value, keys}
      end

      node_id = {:test_source, System.unique_integer()}
      :ok = NodeDAG.register_source(node_id, keys, load_fn)

      event = %Ev{id: 1, tenant_id: 1}
      Enum.each(1..50, fn _ -> NodeDAG.notify(event) end)

      :ok = NodeDAG.drain()

      assert_receive {:dag_value, ^node_id, :loaded_value}, 1_000
      assert_receive {:telemetry, [:upkeep, :node_dag, :dispatch, :start], %{system_time: _},
                      %{node_id: ^node_id, node_kind: :source, pid_count: 1, shard: shard}}

      assert is_integer(shard)

      assert_receive {:telemetry, [:upkeep, :node_dag, :dispatch, :stop],
                      %{duration: duration},
                      %{node_id: ^node_id, node_kind: :source, pid_count: 1}}

      assert is_integer(duration)

      # Coalescing: 50 publishes of an equal-affected node collapse during flush.
      # We tolerate up to a small handful of loads (one per flush window).
      assert :counters.get(counter, 1) <= 5

      NodeDAG.unregister(node_id)
    end

    test "load_fn returning new keys reconciles the index" do
      key_a = narrow_key(%Ev{id: 1, tenant_id: 1})
      key_b = narrow_key(%Ev{id: 1, tenant_id: 2})
      counter = :counters.new(1, [:atomics])

      # First load advertises key_a only; second load advertises key_b only.
      load_fn = fn ->
        next = :counters.add(counter, 1, 1)
        keys = if next == 1, do: [key_a], else: [key_b]
        {:v, keys}
      end

      node_id = {:reregister_test, System.unique_integer()}
      :ok = NodeDAG.register_source(node_id, [key_a], load_fn)

      # First flush: routes via key_a.
      NodeDAG.notify(%Ev{id: 1, tenant_id: 1})
      :ok = NodeDAG.drain()
      assert_receive {:dag_value, ^node_id, :v}, 1_000

      # After re-registration, key_a no longer routes; key_b does.
      assert :ets.lookup(NodeDAG.index_table(), key_a) == []

      NodeDAG.notify(%Ev{id: 1, tenant_id: 2})
      :ok = NodeDAG.drain()
      assert_receive {:dag_value, ^node_id, :v}, 1_000

      NodeDAG.unregister(node_id)
    end

    test "unregistering removes interest and the index" do
      key = narrow_key(%Ev{id: 99, tenant_id: 1})
      load_fn = fn -> {:v, [key]} end
      node_id = {:remove_test, System.unique_integer()}

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
    test "recompute once when source changes; multi-shard, derived auto-colocates with source" do
      source_loads = :counters.new(1, [:atomics])
      derived_computes = :counters.new(1, [:atomics])

      source_id = {:src, System.unique_integer()}
      # Deliberately picked so phash2(derived_id) != phash2(source_id) —
      # tests that register_derived/3 routes to the source's shard, not
      # the derived's hash.
      derived_id = {:der, System.unique_integer()}
      keys = [narrow_key(%Ev{id: 7, tenant_id: 1})]

      load_fn = fn ->
        :counters.add(source_loads, 1, 1)
        {42, keys}
      end

      compute_fn = fn deps ->
        :counters.add(derived_computes, 1, 1)
        Map.fetch!(deps, source_id) * 2
      end

      :ok = NodeDAG.register_source(source_id, keys, load_fn)
      :ok = NodeDAG.register_derived(derived_id, [source_id], compute_fn)

      Enum.each(1..20, fn _ -> NodeDAG.notify(%Ev{id: 7, tenant_id: 1}) end)
      :ok = NodeDAG.drain()

      assert_receive {:dag_value, ^source_id, 42}, 1_000
      assert_receive {:dag_value, ^derived_id, 84}, 1_000

      assert :counters.get(source_loads, 1) <= 5
      assert :counters.get(derived_computes, 1) <= 5

      NodeDAG.unregister(derived_id)
      NodeDAG.unregister(source_id)
    end

    test "raises when deps span multiple shards" do
      shards = :persistent_term.get({NodeDAG, :shards})

      # Find two source ids whose phash2 lands on different shards.
      [src_a, src_b] = pick_split_pair(shards)

      keys_a = [narrow_key(%Ev{id: 1, tenant_id: 1})]
      keys_b = [narrow_key(%Ev{id: 2, tenant_id: 1})]

      :ok = NodeDAG.register_source(src_a, keys_a, fn -> {nil, keys_a} end)
      :ok = NodeDAG.register_source(src_b, keys_b, fn -> {nil, keys_b} end)

      assert_raise ArgumentError, ~r/split across shards/, fn ->
        NodeDAG.register_derived(
          {:der_split, System.unique_integer()},
          [src_a, src_b],
          fn _ -> :unreachable end
        )
      end

      NodeDAG.unregister(src_a)
      NodeDAG.unregister(src_b)
    end
  end

  defp pick_split_pair(shards) when shards >= 2 do
    a = {:src_a, System.unique_integer()}

    b =
      Stream.iterate(1, &(&1 + 1))
      |> Enum.find_value(fn n ->
        candidate = {:src_b, n}

        if :erlang.phash2(candidate, shards) != :erlang.phash2(a, shards),
          do: candidate
      end)

    [a, b]
  end

  defp attach_telemetry(events) do
    test_pid = self()
    handler_id = {__MODULE__, test_pid, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
