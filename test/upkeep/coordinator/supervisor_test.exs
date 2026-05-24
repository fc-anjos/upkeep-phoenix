defmodule Upkeep.Coordinator.SupervisorTest do
  use ExUnit.Case, async: false

  alias Upkeep.Coordinator.Topology

  setup do
    Upkeep.Test.reset_graph()
    on_exit(fn -> Upkeep.Test.reset_graph() end)
    :ok
  end

  test "topology tables are owned by the dedicated owner, not the supervisor" do
    owner = wait_for_owner(Upkeep.Coordinator.Topology.TableOwner)

    # The dedicated owner — not the supervisor process — owns the tables.
    [{nodes_table, _opts} | _] = Topology.table_specs()
    assert :ets.info(nodes_table, :owner) == owner
  end

  test "a dedicated heir keeper is wired up so tables can outlive an owner crash" do
    # The owner has a heir keeper (started before it) configured as the ETS
    # heir for every topology table. Crash-survival behavior itself is covered
    # in isolation by Upkeep.ETS.TableOwnerTest; here we only assert the wiring,
    # because killing the shared, application-wide owner has side effects on
    # every other test in the suite.
    owner = wait_for_owner(Upkeep.Coordinator.Topology.TableOwner)
    keeper = wait_for_owner(Upkeep.ETS.TableOwner.heir_name(Upkeep.Coordinator.Topology.TableOwner))

    assert is_pid(keeper)
    assert keeper != owner

    Enum.each(Topology.table_specs(), fn {table, _opts} ->
      assert :ets.info(table, :heir) == keeper
    end)
  end

  defp wait_for_owner(name) do
    wait_until(fn -> is_pid(Process.whereis(name)) end)
    Process.whereis(name)
  end

  defp wait_until(fun, timeout \\ 2_000) do
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
