defmodule Upkeep.Internal.SingleFlightTest do
  use ExUnit.Case, async: true

  alias Upkeep.Internal.SingleFlight

  test "join returns :no_load when nothing is in flight" do
    assert SingleFlight.join(SingleFlight.new(), :k, {self(), make_ref()}) == :no_load
  end

  test "start + join + pop reaches every waiter exactly once" do
    ref = make_ref()
    from1 = {self(), make_ref()}
    from2 = {self(), make_ref()}

    coalescer =
      SingleFlight.new()
      |> SingleFlight.start(:k, ref, from1, %{snapshot: :v1})

    {:joined, load, coalescer} = SingleFlight.join(coalescer, :k, from2)
    assert load.waiters == [from2, from1]

    {:ok, key, load, coalescer} = SingleFlight.pop(coalescer, ref)
    assert key == :k
    assert load.extra == %{snapshot: :v1}
    assert Enum.sort(load.waiters) == Enum.sort([from1, from2])

    # Refs and loads cleared.
    assert SingleFlight.pop(coalescer, ref) == :stale
    assert SingleFlight.join(coalescer, :k, from1) == :no_load
  end

  test "pop on unknown ref is :stale" do
    assert SingleFlight.pop(SingleFlight.new(), make_ref()) == :stale
  end

  test "two keys coexist independently" do
    ref_a = make_ref()
    ref_b = make_ref()

    coalescer =
      SingleFlight.new()
      |> SingleFlight.start(:a, ref_a, {self(), make_ref()})
      |> SingleFlight.start(:b, ref_b, {self(), make_ref()})

    {:ok, :a, _, coalescer} = SingleFlight.pop(coalescer, ref_a)
    assert {:ok, :b, _, _} = SingleFlight.pop(coalescer, ref_b)
  end
end
