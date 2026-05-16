defmodule Upkeep.Coordinator.SourceProcessTest do
  use ExUnit.Case, async: false

  import Upkeep.TestSupport, only: [attach_telemetry: 1]

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Notifier
  alias Upkeep.Coordinator.SourceProcesses
  alias Upkeep.InvalidationSurface
  alias Upkeep.TestSupport.{DagMessages, TelemetryMessages}

  defmodule Ev do
    defstruct [:id, :tenant_id]
  end

  setup do
    old_ttl = Application.get_env(:upkeep, :source_idle_ttl_ms)
    Application.put_env(:upkeep, :source_idle_ttl_ms, 1_000)
    Upkeep.Test.reset_graph()

    on_exit(fn ->
      restore_ttl(old_ttl)
      Upkeep.Test.reset_graph()
    end)

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

  describe "process-backed source nodes" do
    test "duplicate invalidations coalesce into one source process load" do
      attach_telemetry([[:upkeep, :graph, :notifier, :flush]])

      counter = :counters.new(1, [:atomics])
      event = %Ev{id: 1, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:process_source, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, surface, fn ->
          :counters.add(counter, 1, 1)
          {:loaded_value, surface}
        end)

      with_suspended_notifier(fn ->
        Enum.each(1..12, fn _ -> notify(event) end)
      end)

      :ok = Graph.drain()

      TelemetryMessages.assert_counted([:upkeep, :graph, :notifier, :flush],
        message_count: 12,
        event_count: 1,
        source_node_count: 1,
        source_process_count: 1
      )

      assert DagMessages.receive_value(node_id) == :loaded_value
      assert :counters.get(counter, 1) == 1

      Graph.unregister(node_id)
    end

    test "a slow source process does not block an unrelated fast source process" do
      parent = self()

      slow_event = %Ev{id: 2, tenant_id: 1}
      fast_event = %Ev{id: 3, tenant_id: 1}
      slow_surface = event_surface(slow_event)
      fast_surface = event_surface(fast_event)
      slow_id = {:slow_process_source, System.unique_integer()}
      fast_id = {:fast_process_source, System.unique_integer()}

      :ok =
        Graph.register_loader(slow_id, slow_surface, fn ->
          send(parent, {:slow_started, self()})

          receive do
            :finish_slow -> {:slow_value, slow_surface}
          after
            5_000 -> raise "timed out waiting for slow source release"
          end
        end)

      :ok =
        Graph.register_loader(fast_id, fast_surface, fn ->
          send(parent, :fast_loaded)
          {:fast_value, fast_surface}
        end)

      notify(slow_event)
      :ok = Notifier.drain()
      assert_receive {:slow_started, slow_loader}, 5_000

      notify(fast_event)
      :ok = Notifier.drain()

      assert_receive :fast_loaded, 1_000
      assert DagMessages.receive_value(fast_id) == :fast_value
      DagMessages.refute_value(slow_id, 0)

      send(slow_loader, :finish_slow)
      :ok = Graph.drain()
      assert DagMessages.receive_value(slow_id) == :slow_value

      Graph.unregister(fast_id)
      Graph.unregister(slow_id)
    end

    test "a refresh racing a later invalidation reloads before dispatch" do
      counter = :counters.new(1, [:atomics])
      parent = self()
      event = %Ev{id: 4, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:process_generation_guard, System.unique_integer()}

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
      :ok = Notifier.drain()

      assert_receive {:refresh_started, loader_pid}, 5_000

      notify(event)
      :ok = Notifier.drain()

      send(loader_pid, :finish_refresh)
      :ok = Graph.drain()

      assert DagMessages.receive_value(node_id) == :fresh_value
      DagMessages.refute_any()
      assert :counters.get(counter, 1) == 2

      Graph.unregister(node_id)
    end

    test "a source process returning a new surface updates the routing index" do
      event_a = %Ev{id: 5, tenant_id: 1}
      event_b = %Ev{id: 5, tenant_id: 2}
      surface_a = event_surface(event_a)
      surface_b = event_surface(event_b)
      counter = :counters.new(1, [:atomics])
      node_id = {:process_reindex_source, System.unique_integer()}

      :ok =
        Graph.register_loader(node_id, surface_a, fn ->
          :counters.add(counter, 1, 1)
          next = :counters.get(counter, 1)
          surface = if next == 1, do: surface_b, else: surface_a
          {:value, surface}
        end)

      notify(event_a)
      :ok = Graph.drain()
      assert DagMessages.receive_value(node_id) == :value

      notify(event_b)
      :ok = Graph.drain()
      assert DagMessages.receive_value(node_id) == :value
      assert :counters.get(counter, 1) == 2

      Graph.unregister(node_id)
    end

    test "one-off source processes stop immediately when the last subscriber leaves" do
      surface = event_surface(%Ev{id: 6, tenant_id: 1})
      node_id = {:one_off_process_source, System.unique_integer()}

      :ok = Graph.register_loader(node_id, surface, fn -> {:value, surface} end)
      assert SourceProcesses.count() == 1

      Graph.unregister(node_id)
      wait_until(fn -> SourceProcesses.count() == 0 end)
      refute Graph.registered?(node_id)
    end

    test "shared loaded source processes retain while idle, mark stale, and expire by TTL" do
      Application.put_env(:upkeep, :source_idle_ttl_ms, 25)

      counter = :counters.new(1, [:atomics])
      event = %Ev{id: 7, tenant_id: 1}
      surface = event_surface(event)
      node_id = {:retained_process_source, System.unique_integer()}
      parent = self()

      load_fn = fn ->
        :counters.add(counter, 1, 1)
        {:loaded_value, surface}
      end

      :ok = Graph.register_loader(node_id, surface, load_fn)

      subscriber =
        spawn_link(fn ->
          :ok = Graph.register_loader(node_id, surface, load_fn)
          send(parent, :subscriber_ready)

          receive do
            :unsubscribe -> Graph.unregister(node_id)
          end
        end)

      assert_receive :subscriber_ready, 1_000

      notify(event)
      :ok = Graph.drain()
      assert DagMessages.receive_value(node_id) == :loaded_value
      assert :counters.get(counter, 1) == 1

      send(subscriber, :unsubscribe)
      wait_until(fn -> Graph.subscribed?(node_id, subscriber) == false end)
      Graph.unregister(node_id)

      assert SourceProcesses.count() == 1
      assert Graph.registered?(node_id)

      notify(event)
      :ok = Graph.drain()
      DagMessages.refute_value(node_id, 0)
      assert :counters.get(counter, 1) == 1

      wait_until(fn -> SourceProcesses.count() == 0 end, 1_000)
      refute Graph.registered?(node_id)
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

  defp restore_ttl(nil), do: Application.delete_env(:upkeep, :source_idle_ttl_ms)
  defp restore_ttl(ttl), do: Application.put_env(:upkeep, :source_idle_ttl_ms, ttl)

  defp wait_until(fun, timeout \\ 250) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition was not met before timeout")

      true ->
        Process.sleep(5)
        do_wait_until(fun, deadline)
    end
  end
end
