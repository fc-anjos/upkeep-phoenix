defmodule Upkeep.ETS.TableOwnerTest do
  use ExUnit.Case, async: false

  alias Upkeep.ETS.TableOwner

  defp unique_table(prefix), do: :"#{prefix}_#{System.unique_integer([:positive])}"
  defp unique_name(prefix), do: :"#{prefix}_#{System.unique_integer([:positive])}"

  defp start_owner_tree(table) do
    name = unique_name(:owner)
    specs = TableOwner.child_specs(name: name, tables: [{table, [:set, :public, :named_table]}])
    sup = start_supervised!(%{id: name, start: {Supervisor, :start_link, [specs, [strategy: :one_for_one]]}})
    {name, sup}
  end

  test "creates and owns the named tables on boot" do
    table = unique_table(:owner_creates)
    {name, _sup} = start_owner_tree(table)

    owner = Process.whereis(name)
    assert is_pid(owner)
    assert :ets.info(table, :owner) == owner
    assert :ets.info(table, :name) == table
  end

  test "tables survive an owner crash and are handed back to the restarted owner" do
    table = unique_table(:owner_survives)
    {name, _sup} = start_owner_tree(table)

    owner = Process.whereis(name)
    :ets.insert(table, {:k, :v})

    crash_owner(name)

    # After the keeper hands the table back, the restarted owner owns it again
    # and the data is intact.
    new_owner = wait_for_new_owner(name, owner)
    wait_until(fn -> :ets.info(table, :owner) == new_owner end)
    assert :ets.lookup(table, :k) == [{:k, :v}]
  end

  test "tables survive repeated owner crashes" do
    table = unique_table(:owner_repeat)
    {name, _sup} = start_owner_tree(table)
    :ets.insert(table, {:count, 0})

    Enum.each(1..3, fn _ ->
      previous = Process.whereis(name)
      crash_owner(name)
      new_owner = wait_for_new_owner(name, previous)
      wait_until(fn -> :ets.info(table, :owner) == new_owner end)
    end)

    assert :ets.lookup(table, :count) == [{:count, 0}]
  end

  defp crash_owner(name) do
    owner = Process.whereis(name)
    ref = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^ref, :process, ^owner, :killed}, 5_000
  end

  defp wait_for_new_owner(name, old_owner) do
    wait_until(fn ->
      case Process.whereis(name) do
        nil -> false
        pid when pid != old_owner -> true
        _ -> false
      end
    end)

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
