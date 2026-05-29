defmodule Upkeep.Invalidation.TombstoneTest do
  use ExUnit.Case, async: false

  alias Upkeep.Change
  alias Upkeep.Invalidation.{Bus, Tombstone}

  defp event, do: Change.changed(:thing_changed, %{id: System.unique_integer([:positive])})

  defp now, do: System.system_time(:millisecond)

  test "record/1 stores a dispatched invalidation" do
    e = event()
    assert :ok = Tombstone.record(e)
    assert Enum.any?(Tombstone.entries(), &match?({^e, _ts}, &1))
  end

  test "purge_expired drops entries older than the cutoff and keeps fresh ones" do
    fresh = event()
    stale = event()

    Tombstone.record(fresh)
    :ets.insert(Tombstone.table(), {stale, 1})

    Tombstone.purge_expired(now() - 100)

    assert Enum.any?(Tombstone.entries(), &match?({^fresh, _}, &1))
    refute Enum.any?(Tombstone.entries(), &match?({^stale, _}, &1))
  end

  test "a sync reply replays missed invalidations locally and skips known ones" do
    :ok = Bus.join(:test_subscriber)
    on_exit(fn -> Bus.leave() end)

    missed = event()
    send(Process.whereis(Tombstone), {:tombstone_sync_reply, [{missed, now()}]})
    assert_receive {:upkeep_invalidation, ^missed}, 500

    send(Process.whereis(Tombstone), {:tombstone_sync_reply, [{missed, now()}]})
    refute_receive {:upkeep_invalidation, ^missed}, 200
  end
end
