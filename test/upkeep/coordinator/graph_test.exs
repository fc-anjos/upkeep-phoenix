defmodule Upkeep.Coordinator.GraphTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Upkeep.TestSupport, only: [attach_telemetry: 1]

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Notifier
  alias Upkeep.Invalidation.Bus
  alias Upkeep.InvalidationSurface
  alias Upkeep.Source.Instance
  alias Upkeep.TestSupport.{DagMessages, TelemetryMessages}

  defmodule Ev do
    defstruct [:id, :tenant_id]
  end

  defmodule NoRetrySource do
    use Upkeep.Source, retry: false

    def load(params) do
      count =
        :ets.update_counter(params.table, {:loads, params.id}, {2, 1}, {{:loads, params.id}, 0})

      case count do
        1 -> :stable_value
        _ -> raise "no retry source failed"
      end
    end

    invalidated_by(Upkeep.Coordinator.GraphTest.Ev, on: [:id, :tenant_id])
  end

  defmodule OneRetrySource do
    use Upkeep.Source, retry: [max_attempts: 1, base_delay_ms: 0, max_delay_ms: 0]

    def load(params) do
      count =
        :ets.update_counter(params.table, {:loads, params.id}, {2, 1}, {{:loads, params.id}, 0})

      case count do
        1 -> :stable_value
        _ -> raise "one retry source failed"
      end
    end

    invalidated_by(Upkeep.Coordinator.GraphTest.Ev, on: [:id, :tenant_id])
  end

  setup tags do
    _ = tags
    Upkeep.Test.reset_graph()
    :ok
  end

  defp event_surface(%Ev{} = event), do: event_surface([event])

  defp event_surface(events) when is_list(events) do
    matching_events =
      events
      |> Enum.map(fn %Ev{id: id, tenant_id: tenant_id} -> {id, tenant_id} end)
      |> MapSet.new()

    InvalidationSurface.manual([{:upkeep_event, Ev}], fn
      %Ev{id: id, tenant_id: tenant_id} -> MapSet.member?(matching_events, {id, tenant_id})
      _event -> false
    end)
  end

  defp notify(event), do: Upkeep.Invalidation.dispatch(event)

  describe "source nodes" do
    test "invalidation dispatch reaches the invalidation bus" do
      event = %Ev{id: 200, tenant_id: 1}

      assert Group.member_count(Bus.group(), Bus.key()) >= 2

      :ok = Group.join(Bus.group(), Bus.key(), %{role: :test_subscriber})

      notify(event)

      assert_receive {:upkeep_invalidation, _origin, ^event}, 1_000

      :ok = Group.leave(Bus.group(), Bus.key())
    end

    test "load_fn runs once per coalesced event; subscribers receive {:dag_values, ...}" do
      attach_telemetry([
        [:upkeep, :graph, :invalidation],
        [:upkeep, :graph, :notifier, :flush],
        [:upkeep, :graph, :dispatch, :start],
        [:upkeep, :graph, :dispatch, :stop]
      ])

      counter = :counters.new(1, [:atomics])

      surface = event_surface(%Ev{id: 1, tenant_id: 1})

      load_fn = fn ->
        :counters.add(counter, 1, 1)
        {:loaded_value, surface}
      end

      node_id = {:test_source, System.unique_integer()}
      :ok = Graph.register_loader(node_id, surface, load_fn)

      event = %Ev{id: 1, tenant_id: 1}

      with_suspended_notifier(fn ->
        Enum.each(1..12, fn _ -> notify(event) end)
      end)

      :ok = Graph.drain()

      TelemetryMessages.assert_event([:upkeep, :graph, :invalidation],
        measurements: %{
          count: 1,
          candidate_key_count: 1,
          candidate_count: 1,
          matched_count: 1
        },
        metadata: %{kind: :event, event_module: Ev}
      )

      {%{duration: flush_duration}, _metadata} =
        TelemetryMessages.assert_counted([:upkeep, :graph, :notifier, :flush],
          message_count: 12,
          event_count: 1,
          source_node_count: 1,
          source_process_count: 1
        )

      assert is_integer(flush_duration)

      assert DagMessages.receive_value(node_id) == :loaded_value

      {%{system_time: system_time},
       %{backend: :source_process, partition: partition, pair_count: pair_count}} =
        TelemetryMessages.assert_event([:upkeep, :graph, :dispatch, :start])

      assert is_integer(system_time)
      assert partition != nil
      assert pair_count >= 1

      {%{duration: duration}, %{pid_count: pid_count}} =
        TelemetryMessages.assert_event([:upkeep, :graph, :dispatch, :stop])

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
      surface = event_surface([event_a, event_b])
      node_id = {:same_source_notifier_batch, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, surface, fn ->
          :counters.add(counter, 1, 1)
          {:loaded, surface}
        end)

      with_suspended_notifier(fn ->
        notify(event_a)
        notify(event_b)
      end)

      :ok = Graph.drain()

      TelemetryMessages.assert_counted([:upkeep, :graph, :notifier, :flush],
        message_count: 2,
        event_count: 2,
        source_node_count: 1,
        source_process_count: 1
      )

      assert DagMessages.receive_value(node_id) == :loaded
      assert :counters.get(counter, 1) == 1

      Graph.unregister(node_id)
    end

    test "notifier routes local invalidation bus events" do
      counter = :counters.new(1, [:atomics])
      event = %Ev{id: 203, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:local_bus_event_routed, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, surface, fn ->
          :counters.add(counter, 1, 1)
          {:loaded, surface}
        end)

      send(Process.whereis(Notifier), {:upkeep_invalidation, node(), event})
      :ok = Graph.drain()

      assert :counters.get(counter, 1) == 1
      assert DagMessages.receive_value(node_id) == :loaded

      Graph.unregister(node_id)
    end

    test "notifier routes remote invalidation bus events" do
      attach_telemetry([[:upkeep, :graph, :notifier, :flush]])

      counter = :counters.new(1, [:atomics])
      event = %Ev{id: 204, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:remote_event_routed, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, surface, fn ->
          :counters.add(counter, 1, 1)
          {:loaded, surface}
        end)

      send(Process.whereis(Notifier), {:upkeep_invalidation, :remote@nohost, event})
      :ok = Graph.drain()

      TelemetryMessages.assert_counted([:upkeep, :graph, :notifier, :flush],
        message_count: 1,
        event_count: 1,
        source_node_count: 1,
        source_process_count: 1
      )

      assert DagMessages.receive_value(node_id) == :loaded
      assert :counters.get(counter, 1) == 1

      Graph.unregister(node_id)
    end

    test "refresh results that race a later invalidation are reloaded before dispatch" do
      counter = :counters.new(1, [:atomics])
      parent = self()
      event = %Ev{id: 205, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:generation_guarded_refresh, System.unique_integer()}

      load_fn = fn ->
        :counters.add(counter, 1, 1)

        case :counters.get(counter, 1) do
          1 ->
            send(parent, {:refresh_started, self()})

            receive do
              :finish_refresh -> {:stale_value, surface}
            after
              5_000 -> raise "timed out waiting to finish refresh"
            end

          2 ->
            {:fresh_value, surface}
        end
      end

      :ok = Graph.register_loader(node_id, surface, load_fn)

      notify(event)
      drain = Task.async(fn -> Graph.drain() end)

      assert_receive {:refresh_started, loader_pid}, 5_000

      notify(event)
      :ok = Notifier.drain()

      send(loader_pid, :finish_refresh)
      assert Task.await(drain, 5_000) == :ok

      :ok = Graph.drain()

      assert DagMessages.receive_value(node_id) == :fresh_value
      DagMessages.refute_any()
      assert :counters.get(counter, 1) == 2

      Graph.unregister(node_id)
    end

    test "load_fn returning new keys reconciles the index" do
      surface_a = event_surface(%Ev{id: 1, tenant_id: 1})
      surface_b = event_surface(%Ev{id: 1, tenant_id: 2})
      counter = :counters.new(1, [:atomics])

      # First load advertises surface_a only; second load advertises surface_b only.
      load_fn = fn ->
        next = :counters.add(counter, 1, 1)
        surface = if next == 1, do: surface_a, else: surface_b
        {:v, surface}
      end

      node_id = {:reregister_test, System.unique_integer()}
      :ok = Graph.register_loader(node_id, surface_a, load_fn)

      # First flush: routes via key_a.
      notify(%Ev{id: 1, tenant_id: 1})
      :ok = Graph.drain()
      assert DagMessages.receive_value(node_id) == :v

      notify(%Ev{id: 1, tenant_id: 2})
      :ok = Graph.drain()
      assert DagMessages.receive_value(node_id) == :v

      Graph.unregister(node_id)
    end

    test "unregistering removes interest and the index" do
      surface = event_surface(%Ev{id: 99, tenant_id: 1})
      load_fn = fn -> {:v, surface} end
      node_id = {:remove_test, System.unique_integer()}

      :ok = Graph.register_loader(node_id, surface, load_fn)
      :ok = Graph.unregister(node_id)

      # No subscribers; notify must not deliver anything.
      notify(%Ev{id: 99, tenant_id: 1})
      :ok = Graph.drain()
      DagMessages.refute_value(node_id, 0)
    end

    test "reset removes shared source state and allows a fresh registration" do
      surface = event_surface(%Ev{id: 100, tenant_id: 1})
      node_id = {:reset_test, System.unique_integer()}

      :ok = Graph.register_loader(node_id, surface, fn -> {:old, surface} end)
      :ok = Graph.reset()

      notify(%Ev{id: 100, tenant_id: 1})
      :ok = Graph.drain()
      DagMessages.refute_value(node_id, 0)

      :ok = Graph.register_loader(node_id, surface, fn -> {:new, surface} end)

      notify(%Ev{id: 100, tenant_id: 1})
      :ok = Graph.drain()
      assert DagMessages.receive_value(node_id) == :new

      Graph.unregister(node_id)
    end

    test "indexed routing loads only matching source nodes" do
      matching_loads = :counters.new(1, [:atomics])
      unrelated_loads = :counters.new(1, [:atomics])

      matching_surface = event_surface(%Ev{id: 10, tenant_id: 1})

      matching_id = {:matching_source, System.unique_integer()}
      unrelated_ids = for idx <- 1..8, do: {:unrelated_source, idx, System.unique_integer()}

      :ok =
        Graph.register_loader(matching_id, matching_surface, fn ->
          :counters.add(matching_loads, 1, 1)
          {:matched, matching_surface}
        end)

      Enum.each(unrelated_ids, fn node_id ->
        surface = event_surface(%Ev{id: System.unique_integer([:positive]), tenant_id: 999})

        :ok =
          Graph.register_loader(node_id, surface, fn ->
            :counters.add(unrelated_loads, 1, 1)
            {:unrelated, surface}
          end)
      end)

      notify(%Ev{id: 10, tenant_id: 1})
      :ok = Graph.drain()

      assert DagMessages.receive_value(matching_id) == :matched
      DagMessages.refute_any()

      assert :counters.get(matching_loads, 1) == 1
      assert :counters.get(unrelated_loads, 1) == 0

      Graph.unregister(matching_id)
      Enum.each(unrelated_ids, &Graph.unregister/1)
    end

    test "failed refresh retries with backoff and publishes recovered value once" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      loads = :counters.new(1, [:atomics])

      event = %Ev{id: 11, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:failing_refresh_source, System.unique_integer()}

      load_fn = fn ->
        :counters.add(loads, 1, 1)

        case :counters.get(loads, 1) do
          1 -> {:stable_value, surface}
          2 -> raise "refresh failed"
          _ -> {:recovered_value, surface}
        end
      end

      :ok = Graph.register_loader(node_id, surface, load_fn)

      notify(event)
      :ok = Graph.drain()

      assert DagMessages.receive_value(node_id) == :stable_value
      assert :counters.get(loads, 1) == 1

      log =
        capture_log(fn ->
          notify(event)
          :ok = Graph.drain()
        end)

      assert log =~ "refresh failed"

      metadata = receive_source_load_failure()

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

      assert DagMessages.receive_value(node_id) == :recovered_value
      assert :counters.get(loads, 1) == 3

      Graph.unregister(node_id)
    end

    test "automatic retry stops after a bounded budget and resumes on later notification" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      table = :ets.new(:retry_modes, [:set, :public])
      :ets.insert(table, {:mode, :fail})

      loads = :counters.new(1, [:atomics])
      event = %Ev{id: 12, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:bounded_retry_source, System.unique_integer()}

      load_fn = fn ->
        :counters.add(loads, 1, 1)

        case :counters.get(loads, 1) do
          1 ->
            {:stable_value, surface}

          _ ->
            case :ets.lookup(table, :mode) do
              [{:mode, :recover}] -> {:recovered_value, surface}
              _ -> raise "still broken"
            end
        end
      end

      :ok = Graph.register_loader(node_id, surface, load_fn)

      notify(event)
      :ok = Graph.drain()
      assert DagMessages.receive_value(node_id) == :stable_value

      parent = self()

      log =
        capture_log(fn ->
          notify(event)
          :ok = Graph.drain()
          send(parent, {:retry_failures, receive_source_exception_metadata_until_exhausted()})
        end)

      assert log =~ "still broken"
      assert_receive {:retry_failures, failures}, 1_000

      assert Enum.map(failures, & &1.retry_attempt) == [1, 2, 3, 4]
      assert Enum.map(failures, & &1.retry?) == [true, true, true, false]
      assert Enum.all?(failures, &(&1.retry_max_attempts == 3))
      assert [_, _, _, %{retry_delay_ms: nil}] = failures
      assert :counters.get(loads, 1) == 5

      DagMessages.refute_value(node_id, 0)

      :ets.insert(table, {:mode, :recover})
      notify(event)
      :ok = Graph.drain()

      assert DagMessages.receive_value(node_id) == :recovered_value
      assert :counters.get(loads, 1) == 6

      Graph.unregister(node_id)
      :ets.delete(table)
    end

    test "source retry false disables refresh retry" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      table = :ets.new(:no_retry_source, [:set, :public])
      event = %Ev{id: 13, tenant_id: 1}
      params = %{id: event.id, tenant_id: event.tenant_id, table: table}
      instance = Instance.build(NoRetrySource, params)
      node_id = instance.id

      assert {:ok, result} =
               Graph.register_source_and_load(node_id, instance.surface, instance)

      assert result.value == :stable_value
      assert result.tracked_deps == []

      log =
        capture_log(fn ->
          notify(event)
          :ok = Graph.drain()
        end)

      assert log =~ "no retry source failed"

      metadata = receive_source_load_failure()

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

      DagMessages.refute_value(node_id)
      assert :ets.lookup(table, {:loads, event.id}) == [{{:loads, event.id}, 2}]

      Graph.unregister(node_id)
      :ets.delete(table)
    end

    test "source retry options override application retry budget" do
      attach_telemetry([[:upkeep, :graph, :source_load, :exception]])

      table = :ets.new(:one_retry_source, [:set, :public])
      event = %Ev{id: 14, tenant_id: 1}
      params = %{id: event.id, tenant_id: event.tenant_id, table: table}
      instance = Instance.build(OneRetrySource, params)
      node_id = instance.id

      assert {:ok, result} =
               Graph.register_source_and_load(node_id, instance.surface, instance)

      assert result.value == :stable_value
      assert result.tracked_deps == []

      parent = self()

      log =
        capture_log(fn ->
          notify(event)
          :ok = Graph.drain()
          send(parent, {:retry_failures, receive_source_exception_metadata_until_exhausted()})
        end)

      assert log =~ "one retry source failed"
      assert_receive {:retry_failures, failures}, 1_000

      assert Enum.map(failures, & &1.retry_attempt) == [1, 2]
      assert Enum.map(failures, & &1.retry?) == [true, false]
      assert Enum.all?(failures, &(&1.retry_policy == :source))
      assert Enum.all?(failures, &(&1.retry_max_attempts == 1))
      assert [_, %{retry_delay_ms: nil}] = failures
      assert :ets.lookup(table, {:loads, event.id}) == [{{:loads, event.id}, 3}]

      Graph.unregister(node_id)
      :ets.delete(table)
    end
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

  defp receive_source_exception_metadata_until_exhausted(metadata \\ []) do
    next_metadata = receive_source_load_failure()
    metadata = metadata ++ [next_metadata]

    if next_metadata.retry? do
      receive_source_exception_metadata_until_exhausted(metadata)
    else
      metadata
    end
  end

  defp receive_source_load_failure do
    {_measurements, metadata} =
      TelemetryMessages.assert_counted([:upkeep, :graph, :source_load, :exception])

    metadata
  end
end
