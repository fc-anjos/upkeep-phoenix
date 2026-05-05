defmodule Upkeep.Internal.Coordinator.GraphTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Upkeep.Internal.Coordinator.Graph
  alias Upkeep.Internal.Coordinator.Graph.Notifier

  defmodule Ev do
    defstruct [:id, :tenant_id]
  end

  defmodule NoRetrySource do
    use Upkeep.Source, retry: false

    query(fn params ->
      count =
        :ets.update_counter(params.table, {:loads, params.id}, {2, 1}, {{:loads, params.id}, 0})

      case count do
        1 -> :stable_value
        _ -> raise "no retry source failed"
      end
    end)

    invalidated_by(Upkeep.Internal.Coordinator.GraphTest.Ev, on: [:id, :tenant_id])
  end

  defmodule OneRetrySource do
    use Upkeep.Source, retry: [max_attempts: 1, base_delay_ms: 0, max_delay_ms: 0]

    query(fn params ->
      count =
        :ets.update_counter(params.table, {:loads, params.id}, {2, 1}, {{:loads, params.id}, 0})

      case count do
        1 -> :stable_value
        _ -> raise "one retry source failed"
      end
    end)

    invalidated_by(Upkeep.Internal.Coordinator.GraphTest.Ev, on: [:id, :tenant_id])
  end

  setup tags do
    _ = Map.get(tags, :shards, 4)
    Upkeep.Test.reset_graph()
    :ok
  end

  defp narrow_key(%Ev{id: id, tenant_id: tid}) do
    {:upkeep_event, Ev, Enum.sort([{:id, id}, {:tenant_id, tid}])}
  end

  describe "source nodes" do
    test "notify delegates cluster fanout to the Group notification group" do
      event = %Ev{id: 200, tenant_id: 1}

      assert Group.member_count(Graph.group(), Graph.notification_key()) >= 2
      assert Group.member_count(Graph.group(), "graph/shard/") >= Graph.shard_count()

      :ok = Group.join(Graph.group(), Graph.notification_key(), %{role: :probe})

      Graph.notify(event)

      assert_receive {:upkeep_graph_notify, _origin, ^event}, 1_000

      :ok = Group.leave(Graph.group(), Graph.notification_key())
    end

    test "load_fn runs once per coalesced event; subscribers receive {:dag_values, ...}" do
      attach_telemetry([
        [:upkeep, :graph, :notifier, :flush],
        [:upkeep, :graph, :dispatch, :start],
        [:upkeep, :graph, :dispatch, :stop]
      ])

      counter = :counters.new(1, [:atomics])

      keys = [narrow_key(%Ev{id: 1, tenant_id: 1})]

      load_fn = fn ->
        :counters.add(counter, 1, 1)
        {:loaded_value, keys}
      end

      node_id = {:test_source, System.unique_integer()}
      :ok = Graph.register_loader(node_id, keys, load_fn)

      event = %Ev{id: 1, tenant_id: 1}

      with_suspended_notifier(fn ->
        Enum.each(1..12, fn _ -> Graph.notify(event) end)
      end)

      :ok = Graph.drain()

      assert_receive {:telemetry, [:upkeep, :graph, :notifier, :flush], %{count: 1},
                      %{
                        message_count: 12,
                        event_count: 1,
                        source_node_count: 1,
                        shard_count: 1
                      }}

      assert_receive {:dag_values, [{^node_id, :loaded_value}]}, 1_000

      assert_receive {:telemetry, [:upkeep, :graph, :dispatch, :start], %{system_time: _},
                      %{shard: shard, pair_count: pair_count}}

      assert is_integer(shard)
      assert pair_count >= 1

      assert_receive {:telemetry, [:upkeep, :graph, :dispatch, :stop], %{duration: duration},
                      %{pid_count: pid_count}}

      assert is_integer(duration)
      assert pid_count >= 1

      assert :counters.get(counter, 1) == 1

      Graph.unregister(node_id)
    end

    test "notifier batches different events for the same affected source" do
      attach_telemetry([[:upkeep, :graph, :notifier, :flush]])

      counter = :counters.new(1, [:atomics])
      event_a = %Ev{id: 201, tenant_id: 1}
      event_b = %Ev{id: 202, tenant_id: 1}
      keys = [narrow_key(event_a), narrow_key(event_b)]
      node_id = {:same_source_notifier_batch, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, keys, fn ->
          :counters.add(counter, 1, 1)
          {:loaded, keys}
        end)

      with_suspended_notifier(fn ->
        Graph.notify(event_a)
        Graph.notify(event_b)
      end)

      :ok = Graph.drain()

      assert_receive {:telemetry, [:upkeep, :graph, :notifier, :flush], %{count: 1},
                      %{
                        message_count: 2,
                        event_count: 2,
                        source_node_count: 1,
                        shard_count: 1
                      }}

      assert_receive {:dag_values, [{^node_id, :loaded}]}, 1_000
      assert :counters.get(counter, 1) == 1

      Graph.unregister(node_id)
    end

    test "notifier ignores local Group echoes" do
      counter = :counters.new(1, [:atomics])
      event = %Ev{id: 203, tenant_id: 1}
      keys = [narrow_key(event)]
      node_id = {:local_echo_ignored, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, keys, fn ->
          :counters.add(counter, 1, 1)
          {:loaded, keys}
        end)

      send(Process.whereis(Notifier), {:upkeep_graph_notify, node(), event})
      :ok = Graph.drain()

      assert :counters.get(counter, 1) == 0
      refute_received {:dag_values, [{^node_id, _}]}

      Graph.unregister(node_id)
    end

    test "notifier routes remote Group events" do
      attach_telemetry([[:upkeep, :graph, :notifier, :flush]])

      counter = :counters.new(1, [:atomics])
      event = %Ev{id: 204, tenant_id: 1}
      keys = [narrow_key(event)]
      node_id = {:remote_event_routed, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, keys, fn ->
          :counters.add(counter, 1, 1)
          {:loaded, keys}
        end)

      send(Process.whereis(Notifier), {:upkeep_graph_notify, :remote@nohost, event})
      :ok = Graph.drain()

      assert_receive {:telemetry, [:upkeep, :graph, :notifier, :flush], %{count: 1},
                      %{
                        message_count: 1,
                        event_count: 1,
                        source_node_count: 1,
                        shard_count: 1
                      }}

      assert_receive {:dag_values, [{^node_id, :loaded}]}, 1_000
      assert :counters.get(counter, 1) == 1

      Graph.unregister(node_id)
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
      :ok = Graph.register_loader(node_id, [key_a], load_fn)

      # First flush: routes via key_a.
      Graph.notify(%Ev{id: 1, tenant_id: 1})
      :ok = Graph.drain()
      assert_receive {:dag_values, [{^node_id, :v}]}, 1_000

      Graph.notify(%Ev{id: 1, tenant_id: 2})
      :ok = Graph.drain()
      assert_receive {:dag_values, [{^node_id, :v}]}, 1_000

      Graph.unregister(node_id)
    end

    test "unregistering removes interest and the index" do
      key = narrow_key(%Ev{id: 99, tenant_id: 1})
      load_fn = fn -> {:v, [key]} end
      node_id = {:remove_test, System.unique_integer()}

      :ok = Graph.register_loader(node_id, [key], load_fn)
      :ok = Graph.unregister(node_id)

      # No subscribers; notify must not deliver anything.
      Graph.notify(%Ev{id: 99, tenant_id: 1})
      :ok = Graph.drain()
      refute_received {:dag_values, [{^node_id, _}]}
    end

    test "reset removes shared source state and allows a fresh registration" do
      key = narrow_key(%Ev{id: 100, tenant_id: 1})
      node_id = {:reset_test, System.unique_integer()}

      :ok = Graph.register_loader(node_id, [key], fn -> {:old, [key]} end)
      :ok = Graph.reset()

      Graph.notify(%Ev{id: 100, tenant_id: 1})
      :ok = Graph.drain()
      refute_received {:dag_values, [{^node_id, _}]}

      :ok = Graph.register_loader(node_id, [key], fn -> {:new, [key]} end)

      Graph.notify(%Ev{id: 100, tenant_id: 1})
      :ok = Graph.drain()
      assert_receive {:dag_values, [{^node_id, :new}]}, 1_000

      Graph.unregister(node_id)
    end

    test "indexed routing loads only matching source nodes" do
      matching_loads = :counters.new(1, [:atomics])
      unrelated_loads = :counters.new(1, [:atomics])

      matching_key = narrow_key(%Ev{id: 10, tenant_id: 1})

      matching_id = {:matching_source, System.unique_integer()}
      unrelated_ids = for idx <- 1..8, do: {:unrelated_source, idx, System.unique_integer()}

      :ok =
        Graph.register_loader(matching_id, [matching_key], fn ->
          :counters.add(matching_loads, 1, 1)
          {:matched, [matching_key]}
        end)

      Enum.each(unrelated_ids, fn node_id ->
        key = narrow_key(%Ev{id: System.unique_integer([:positive]), tenant_id: 999})

        :ok =
          Graph.register_loader(node_id, [key], fn ->
            :counters.add(unrelated_loads, 1, 1)
            {:unrelated, [key]}
          end)
      end)

      Graph.notify(%Ev{id: 10, tenant_id: 1})
      :ok = Graph.drain()

      assert_receive {:dag_values, [{^matching_id, :matched}]}, 1_000
      refute_received {:dag_values, [{{:unrelated_source, _, _}, _}]}

      assert :counters.get(matching_loads, 1) == 1
      assert :counters.get(unrelated_loads, 1) == 0

      Graph.unregister(matching_id)
      Enum.each(unrelated_ids, &Graph.unregister/1)
    end

    test "failed refresh retries with backoff and publishes recovered value once" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      loads = :counters.new(1, [:atomics])
      derived_computes = :counters.new(1, [:atomics])

      event = %Ev{id: 11, tenant_id: 1}
      key = narrow_key(event)
      node_id = {:failing_refresh_source, System.unique_integer()}
      derived_id = {:failing_refresh_derived, System.unique_integer()}

      load_fn = fn ->
        :counters.add(loads, 1, 1)

        case :counters.get(loads, 1) do
          1 -> {:stable_value, [key]}
          2 -> raise "refresh failed"
          _ -> {:recovered_value, [key]}
        end
      end

      compute_fn = fn deps ->
        :counters.add(derived_computes, 1, 1)
        {:derived, Map.fetch!(deps, node_id)}
      end

      :ok = Graph.register_loader(node_id, [key], load_fn)
      :ok = Graph.register_derived(derived_id, [node_id], compute_fn)

      Graph.notify(event)
      :ok = Graph.drain()

      assert_receive {:dag_values, batch}, 1_000
      assert {node_id, :stable_value} in batch
      assert {derived_id, {:derived, :stable_value}} in batch
      assert :counters.get(loads, 1) == 1
      assert :counters.get(derived_computes, 1) == 1

      log =
        capture_log(fn ->
          Graph.notify(event)
          :ok = Graph.drain()
        end)

      assert log =~ "refresh failed"

      assert_receive {:telemetry, [:upkeep, :graph, :source_load, :exception], %{count: 1},
                      metadata}

      assert %{
               node_id: ^node_id,
               source: nil,
               params: nil,
               exception: RuntimeError,
               subscriber_count: 1,
               retry?: true,
               retry_attempt: 1,
               retry_max_attempts: 3,
               retry_delay_ms: retry_delay_ms,
               reason: {%RuntimeError{message: "refresh failed"}, stacktrace}
             } = metadata

      assert is_list(stacktrace)
      assert is_integer(retry_delay_ms)
      assert retry_delay_ms >= 0

      assert_receive {:dag_values, batch}, 1_000
      assert {node_id, :recovered_value} in batch
      assert {derived_id, {:derived, :recovered_value}} in batch
      assert :counters.get(loads, 1) == 3
      assert :counters.get(derived_computes, 1) == 2

      Graph.unregister(derived_id)
      Graph.unregister(node_id)
    end

    test "automatic retry stops after a bounded budget and resumes on later notification" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      table = :ets.new(:retry_modes, [:set, :public])
      :ets.insert(table, {:mode, :fail})

      loads = :counters.new(1, [:atomics])
      event = %Ev{id: 12, tenant_id: 1}
      key = narrow_key(event)
      node_id = {:bounded_retry_source, System.unique_integer()}

      load_fn = fn ->
        :counters.add(loads, 1, 1)

        case :counters.get(loads, 1) do
          1 ->
            {:stable_value, [key]}

          _ ->
            case :ets.lookup(table, :mode) do
              [{:mode, :recover}] -> {:recovered_value, [key]}
              _ -> raise "still broken"
            end
        end
      end

      :ok = Graph.register_loader(node_id, [key], load_fn)

      Graph.notify(event)
      :ok = Graph.drain()
      assert_receive {:dag_values, [{^node_id, :stable_value}]}, 1_000

      parent = self()

      log =
        capture_log(fn ->
          Graph.notify(event)
          :ok = Graph.drain()
          send(parent, {:retry_failures, receive_source_exception_metadata_until_exhausted()})
        end)

      assert log =~ "still broken"
      assert_receive {:retry_failures, failures}, 1_000

      assert Enum.map(failures, & &1.retry_attempt) == [1, 2, 3, 4]
      assert Enum.map(failures, & &1.retry?) == [true, true, true, false]
      assert Enum.all?(failures, &(&1.retry_max_attempts == 3))
      assert %{retry_delay_ms: nil} = List.last(failures)
      assert :counters.get(loads, 1) == 5

      refute_received {:dag_values, [{^node_id, _}]}

      :ets.insert(table, {:mode, :recover})
      Graph.notify(event)
      :ok = Graph.drain()

      assert_receive {:dag_values, [{^node_id, :recovered_value}]}, 1_000
      assert :counters.get(loads, 1) == 6

      Graph.unregister(node_id)
      :ets.delete(table)
    end

    test "source retry false disables refresh retry" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      table = :ets.new(:no_retry_source, [:set, :public])
      event = %Ev{id: 13, tenant_id: 1}
      params = %{id: event.id, tenant_id: event.tenant_id, table: table}
      node_id = {NoRetrySource, params}
      interest_keys = NoRetrySource.__upkeep_interest_keys__(params)

      assert {:ok, :stable_value, []} =
               Graph.register_source_and_load(node_id, interest_keys, NoRetrySource, params)

      log =
        capture_log(fn ->
          Graph.notify(event)
          :ok = Graph.drain()
        end)

      assert log =~ "no retry source failed"

      assert_receive {:telemetry, [:upkeep, :graph, :source_load, :exception], %{count: 1},
                      metadata}

      assert %{
               node_id: ^node_id,
               source: NoRetrySource,
               params: ^params,
               retry?: false,
               retry_policy: :none,
               retry_attempt: 1,
               retry_max_attempts: 0,
               retry_delay_ms: nil
             } = metadata

      refute_receive {:dag_values, [{^node_id, _}]}, 100
      assert :ets.lookup(table, {:loads, event.id}) == [{{:loads, event.id}, 2}]

      Graph.unregister(node_id)
      :ets.delete(table)
    end

    test "source retry options override application retry budget" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      table = :ets.new(:one_retry_source, [:set, :public])
      event = %Ev{id: 14, tenant_id: 1}
      params = %{id: event.id, tenant_id: event.tenant_id, table: table}
      node_id = {OneRetrySource, params}
      interest_keys = OneRetrySource.__upkeep_interest_keys__(params)

      assert {:ok, :stable_value, []} =
               Graph.register_source_and_load(node_id, interest_keys, OneRetrySource, params)

      parent = self()

      log =
        capture_log(fn ->
          Graph.notify(event)
          :ok = Graph.drain()
          send(parent, {:retry_failures, receive_source_exception_metadata_until_exhausted()})
        end)

      assert log =~ "one retry source failed"
      assert_receive {:retry_failures, failures}, 1_000

      assert Enum.map(failures, & &1.retry_attempt) == [1, 2]
      assert Enum.map(failures, & &1.retry?) == [true, false]
      assert Enum.all?(failures, &(&1.retry_policy == :source))
      assert Enum.all?(failures, &(&1.retry_max_attempts == 1))
      assert %{retry_delay_ms: nil} = List.last(failures)
      assert :ets.lookup(table, {:loads, event.id}) == [{{:loads, event.id}, 3}]

      Graph.unregister(node_id)
      :ets.delete(table)
    end
  end

  describe "derived nodes" do
    test "source loads and derived recomputes are shared across many subscribers" do
      source_loads = :counters.new(1, [:atomics])
      derived_computes = :counters.new(1, [:atomics])

      source_id = {:shared_source, System.unique_integer()}
      derived_id = {:shared_derived, System.unique_integer()}
      keys = [narrow_key(%Ev{id: 20, tenant_id: 1})]

      load_fn = fn ->
        :counters.add(source_loads, 1, 1)
        {[:a, :b], keys}
      end

      compute_fn = fn deps ->
        :counters.add(derived_computes, 1, 1)
        deps |> Map.fetch!(source_id) |> length()
      end

      :ok = Graph.register_loader(source_id, keys, load_fn)
      :ok = Graph.register_derived(derived_id, [source_id], compute_fn)

      subscribers =
        start_subscribers(12, fn ->
          :ok = Graph.register_loader(source_id, keys, load_fn)
          :ok = Graph.register_derived(derived_id, [source_id], compute_fn)
        end)

      Graph.notify(%Ev{id: 20, tenant_id: 1})
      :ok = Graph.drain()

      assert_receive {:dag_values, batch}, 1_000
      assert {source_id, [:a, :b]} in batch
      assert {derived_id, 2} in batch

      assert :counters.get(source_loads, 1) <= 5
      assert :counters.get(derived_computes, 1) <= 5
      assert :counters.get(derived_computes, 1) == :counters.get(source_loads, 1)

      Graph.unregister(derived_id)
      Graph.unregister(source_id)
      stop_subscribers(subscribers)
    end

    test "duplicate events coalesce before source load and derived recompute" do
      source_loads = :counters.new(1, [:atomics])
      derived_computes = :counters.new(1, [:atomics])

      source_id = {:coalesced_source, System.unique_integer()}
      derived_id = {:coalesced_derived, System.unique_integer()}
      event = %Ev{id: 21, tenant_id: 1}
      keys = [narrow_key(event)]

      load_fn = fn ->
        :counters.add(source_loads, 1, 1)
        {:value, keys}
      end

      compute_fn = fn deps ->
        :counters.add(derived_computes, 1, 1)
        Map.fetch!(deps, source_id)
      end

      :ok = Graph.register_loader(source_id, keys, load_fn)
      :ok = Graph.register_derived(derived_id, [source_id], compute_fn)

      Enum.each(1..12, fn _ -> Graph.notify(event) end)
      :ok = Graph.drain()

      assert_receive {:dag_values, batch}, 1_000
      assert {source_id, :value} in batch
      assert {derived_id, :value} in batch

      assert :counters.get(source_loads, 1) <= 5
      assert :counters.get(derived_computes, 1) <= 5
      assert :counters.get(derived_computes, 1) == :counters.get(source_loads, 1)

      Graph.unregister(derived_id)
      Graph.unregister(source_id)
    end

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

      :ok = Graph.register_loader(source_id, keys, load_fn)
      :ok = Graph.register_derived(derived_id, [source_id], compute_fn)

      Enum.each(1..12, fn _ -> Graph.notify(%Ev{id: 7, tenant_id: 1}) end)
      :ok = Graph.drain()

      assert_receive {:dag_values, batch}, 1_000
      assert {source_id, 42} in batch
      assert {derived_id, 84} in batch

      assert :counters.get(source_loads, 1) <= 5
      assert :counters.get(derived_computes, 1) <= 5

      Graph.unregister(derived_id)
      Graph.unregister(source_id)
    end

    test "raises with a dependency plan when deps span multiple shards" do
      shards = Graph.shard_count()

      [src_a, src_b, src_c] = pick_colocated_split_triple(shards)

      keys_a = [narrow_key(%Ev{id: 1, tenant_id: 1})]
      keys_b = [narrow_key(%Ev{id: 2, tenant_id: 1})]
      keys_c = [narrow_key(%Ev{id: 3, tenant_id: 1})]

      :ok = Graph.register_loader(src_a, keys_a, fn -> {nil, keys_a} end)
      :ok = Graph.register_loader(src_b, keys_b, fn -> {nil, keys_b} end)
      :ok = Graph.register_loader(src_c, keys_c, fn -> {nil, keys_c} end)

      error =
        assert_raise ArgumentError, fn ->
          Graph.register_derived(
            {:der_split, System.unique_integer()},
            [src_a, src_b, src_c],
            fn _ -> :unreachable end
          )
        end

      message = Exception.message(error)

      assert message =~ "deps split across shards"
      assert message =~ "largest colocated dependency group"
      assert message =~ inspect(src_a)
      assert message =~ inspect(src_c)
      assert message =~ "cross-shard recompute is not implemented"

      Graph.unregister(src_a)
      Graph.unregister(src_b)
      Graph.unregister(src_c)
    end
  end

  defp pick_colocated_split_triple(shards) when shards >= 2 do
    candidates = Stream.map(1..10_000, &{:src, &1, System.unique_integer()})

    groups =
      candidates
      |> Enum.take(10_000)
      |> Enum.group_by(&generic_node_shard(&1, shards))

    {same_shard, [a, c | _]} = Enum.find(groups, fn {_shard, ids} -> length(ids) >= 2 end)

    {_other_shard, [b | _]} =
      Enum.find(groups, fn {shard, ids} -> shard != same_shard and ids != [] end)

    [a, b, c]
  end

  defp generic_node_shard(node_id, shards) do
    :erlang.phash2({:node, node_id}, shards)
  end

  defp start_subscribers(count, register_fn) do
    parent = self()

    pids =
      for _ <- 1..count do
        spawn_link(fn ->
          register_fn.()
          send(parent, :ready)
          subscriber_loop()
        end)
      end

    for _ <- 1..count, do: receive(do: (:ready -> :ok))
    pids
  end

  defp stop_subscribers(pids) do
    Enum.each(pids, &send(&1, :stop))
  end

  defp with_suspended_notifier(fun) do
    notifier = Process.whereis(Notifier)
    :ok = :sys.suspend(notifier)

    try do
      fun.()
    after
      :ok = :sys.resume(notifier)
    end
  end

  defp subscriber_loop do
    receive do
      :stop -> :ok
      {:dag_values, _pairs} -> subscriber_loop()
    after
      5_000 -> :ok
    end
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

  defp receive_source_exception_metadata_until_exhausted(metadata \\ []) do
    assert_receive {:telemetry, [:upkeep, :graph, :source_load, :exception], %{count: 1},
                    next_metadata},
                   1_000

    metadata = metadata ++ [next_metadata]

    if next_metadata.retry? do
      receive_source_exception_metadata_until_exhausted(metadata)
    else
      metadata
    end
  end
end
