defmodule Upkeep.Coordinator.LifecycleMonitorTest do
  use ExUnit.Case, async: false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.SourceProcesses
  alias Upkeep.Coordinator.Subscriptions
  alias Upkeep.InvalidationSurface

  defmodule Ev do
    defstruct [:id, :tenant_id]
  end

  setup do
    old_ttl = Application.get_env(:upkeep, :source_idle_ttl_ms)
    Application.put_env(:upkeep, :source_idle_ttl_ms, 25)
    Upkeep.Test.reset_graph()

    on_exit(fn ->
      restore_ttl(old_ttl)
      Upkeep.Test.reset_graph()
    end)

    :ok
  end

  defp event_surface(%Ev{id: id, tenant_id: tenant_id}) do
    InvalidationSurface.manual([{:upkeep_event, Ev}], fn
      %Ev{id: ^id, tenant_id: ^tenant_id} -> true
      _event -> false
    end)
  end

  test "a dying single-subscriber watcher releases (stops) its source process" do
    surface = event_surface(%Ev{id: 100, tenant_id: 1})
    node_id = {:dying_watcher_source, System.unique_integer()}
    parent = self()

    watcher =
      spawn(fn ->
        :ok = Graph.register_loader(node_id, surface, fn -> {:value, surface} end)
        send(parent, :registered)

        receive do
          :die -> :ok
        end
      end)

    assert_receive :registered, 1_000
    assert SourceProcesses.count() == 1
    assert Subscriptions.member_count(Subscriptions.source_key(node_id)) == 1

    ref = Process.monitor(watcher)
    send(watcher, :die)
    assert_receive {:DOWN, ^ref, :process, _, _}, 1_000

    wait_until(fn -> SourceProcesses.count() == 0 end, 2_000)
    refute Graph.registered?(node_id)
    assert Subscriptions.member_count(Subscriptions.source_key(node_id)) == 0
  end

  test "a dying shared-subscriber watcher arms idle/TTL once the last watcher leaves" do
    surface = event_surface(%Ev{id: 101, tenant_id: 1})
    node_id = {:dying_shared_source, System.unique_integer()}
    parent = self()

    spawn_watcher = fn ->
      spawn(fn ->
        :ok = Graph.register_loader(node_id, surface, fn -> {:value, surface} end)
        send(parent, {:registered, self()})

        receive do
          :die -> :ok
        end
      end)
    end

    w1 = spawn_watcher.()
    assert_receive {:registered, ^w1}, 1_000
    w2 = spawn_watcher.()
    assert_receive {:registered, ^w2}, 1_000

    # Touch subscribers so max_subscribers > 1 (retain_idle? path).
    SourceProcesses.touch_subscribers(node_id)
    assert SourceProcesses.count() == 1

    ref2 = Process.monitor(w2)
    send(w2, :die)
    assert_receive {:DOWN, ^ref2, :process, _, _}, 1_000

    ref1 = Process.monitor(w1)
    send(w1, :die)
    assert_receive {:DOWN, ^ref1, :process, _, _}, 1_000

    # With member_count 0 and idle TTL armed by the release path, the process
    # should expire by TTL.
    wait_until(fn -> SourceProcesses.count() == 0 end, 2_000)
    refute Graph.registered?(node_id)
  end

  defp restore_ttl(nil), do: Application.delete_env(:upkeep, :source_idle_ttl_ms)
  defp restore_ttl(ttl), do: Application.put_env(:upkeep, :source_idle_ttl_ms, ttl)

  defp wait_until(fun, timeout) do
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
