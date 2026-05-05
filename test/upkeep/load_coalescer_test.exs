defmodule Upkeep.LoadCoalescerTest do
  use ExUnit.Case, async: true

  alias Upkeep.LoadCoalescer

  test "join returns :no_load when nothing is in flight" do
    assert LoadCoalescer.join(LoadCoalescer.new(), :k, {self(), make_ref()}) == :no_load
  end

  test "start + join + pop reaches every waiter exactly once" do
    ref = make_ref()
    from1 = {self(), make_ref()}
    from2 = {self(), make_ref()}

    coalescer =
      LoadCoalescer.new()
      |> LoadCoalescer.start(:k, ref, from1, %{snapshot: :v1})

    {:joined, load, coalescer} = LoadCoalescer.join(coalescer, :k, from2)
    assert load.waiters == [from2, from1]

    {:ok, key, load, coalescer} = LoadCoalescer.pop(coalescer, ref)
    assert key == :k
    assert load.extra == %{snapshot: :v1}
    assert Enum.sort(load.waiters) == Enum.sort([from1, from2])

    # Refs and loads cleared.
    assert LoadCoalescer.pop(coalescer, ref) == :stale
    assert LoadCoalescer.join(coalescer, :k, from1) == :no_load
  end

  test "pop on unknown ref is :stale" do
    assert LoadCoalescer.pop(LoadCoalescer.new(), make_ref()) == :stale
  end

  test "two keys coexist independently" do
    ref_a = make_ref()
    ref_b = make_ref()

    coalescer =
      LoadCoalescer.new()
      |> LoadCoalescer.start(:a, ref_a, {self(), make_ref()})
      |> LoadCoalescer.start(:b, ref_b, {self(), make_ref()})

    {:ok, :a, _, coalescer} = LoadCoalescer.pop(coalescer, ref_a)
    assert {:ok, :b, _, _} = LoadCoalescer.pop(coalescer, ref_b)
  end
end
