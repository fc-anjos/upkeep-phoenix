defmodule Upkeep.SingleFlight.RegistryTest do
  use ExUnit.Case, async: true

  alias Upkeep.SingleFlight.Registry
  alias Upkeep.SingleFlight.Registry.LoaderDown

  setup do
    name = :"single_flight_test_#{System.unique_integer([:positive])}"
    start_supervised!({Registry, name: name})
    {:ok, name: name}
  end

  test "coalesced waiters share the leader's result", %{name: name} do
    parent = self()
    key = {:share, System.unique_integer()}
    barrier = make_ref()
    n = 4

    pids =
      for i <- 1..n do
        spawn_link(fn ->
          send(parent, {:ready, i})

          receive do
            {^barrier, :go} -> :ok
          end

          value =
            Registry.coalesce(name, key, fn ->
              Process.sleep(20)
              :the_value
            end)

          send(parent, {:done, i, value})
        end)
      end

    for i <- 1..n, do: assert_receive({:ready, ^i}, 5_000)
    Enum.each(pids, fn pid -> send(pid, {barrier, :go}) end)

    for _ <- 1..n do
      assert_receive {:done, _i, :the_value}, 5_000
    end

    refute Registry.pending?(name, key)
  end

  test "a successfully-computed result wins the race against the leader's death", %{name: name} do
    parent = self()
    key = {:race, System.unique_integer()}
    release = make_ref()

    # Waiter joins first and coalesces onto the leader.
    waiter =
      spawn_link(fn ->
        # Give the leader a head start so it reserves the load.
        receive do
          {^release, :join} -> :ok
        end

        send(parent, :waiter_reserving)

        result =
          try do
            {:ok, Registry.coalesce(name, key, fn -> flunk("waiter should not lead") end)}
          rescue
            e in LoaderDown -> {:loader_down, e}
          end

        send(parent, {:waiter_result, result})
      end)

    # Leader reserves the load, computes a value, casts :settle, then is killed
    # *immediately* after coalesce/3 returns. The :settle cast is already in the
    # registry mailbox, but the {:DOWN, ...} for the killed leader may be
    # scheduled ahead of it. The computed value must still win.
    leader =
      spawn(fn ->
        value =
          Registry.coalesce(name, key, fn ->
            send(parent, {:leader_reserved, self()})
            # Wait until the waiter has coalesced so it depends on this result.
            receive do
              {^release, :compute} -> :ok
            end

            :computed_value
          end)

        send(parent, {:leader_value, value})
      end)

    assert_receive {:leader_reserved, leader_pid}, 5_000
    assert leader_pid == leader

    # Let the waiter coalesce onto the in-flight load, and wait until the
    # registry has actually recorded it as a waiter (deterministic join).
    send(waiter, {release, :join})
    assert_receive :waiter_reserving, 5_000
    wait_until(fn -> Registry.waiter_count(name, key) >= 1 end)

    # Let the leader finish computing; it casts :settle then returns and exits.
    send(leader, {release, :compute})

    # Kill the leader as soon as it has computed (it may already be exiting).
    # Either way, the settle cast was enqueued before the DOWN's effect.
    assert_receive {:leader_value, :computed_value}, 5_000
    Process.exit(leader, :kill)

    # The coalesced waiter must receive the computed value, NOT LoaderDown.
    assert_receive {:waiter_result, {:ok, :computed_value}}, 5_000

    wait_until(fn -> not Registry.pending?(name, key) end)
  end

  test "the registry survives a leader killed before it settles", %{name: name} do
    parent = self()
    key = {:killed, System.unique_integer()}
    release = make_ref()

    leader =
      spawn(fn ->
        Registry.coalesce(name, key, fn ->
          send(parent, :leader_in_fun)

          receive do
            {^release, :never} -> :ok
          end
        end)
      end)

    assert_receive :leader_in_fun, 5_000

    # Kill the leader while it is still computing (no settle was ever cast).
    Process.exit(leader, :kill)

    # The registry must clear the dead load (via its DOWN handler) without
    # crashing. Once it is no longer pending, a fresh caller becomes the new
    # leader and computes successfully.
    wait_until(fn -> not Registry.pending?(name, key) end)

    value = Registry.coalesce(name, key, fn -> :fresh_value end)
    assert value == :fresh_value
    refute Registry.pending?(name, key)
  end

  defp wait_until(fun, timeout \\ 1_000) do
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
