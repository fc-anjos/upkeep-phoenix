defmodule Upkeep.Internal.DirtyBufferTest do
  use ExUnit.Case, async: true

  alias Upkeep.Internal.DirtyBuffer

  test "first enqueue schedules; subsequent enqueues wait" do
    buffer = DirtyBuffer.new(threshold: 100)

    assert {:schedule, buffer} = DirtyBuffer.enqueue(buffer, [:a, :b])
    assert DirtyBuffer.size(buffer) == 2

    assert {:wait, buffer} = DirtyBuffer.enqueue(buffer, [:c])
    assert DirtyBuffer.size(buffer) == 3
    assert Enum.sort(DirtyBuffer.members(buffer)) == [:a, :b, :c]
  end

  test "threshold triggers :flush_now" do
    buffer = DirtyBuffer.new(threshold: 3)

    {:flush_now, buffer} = DirtyBuffer.enqueue(buffer, [:a, :b, :c])
    assert DirtyBuffer.size(buffer) == 3
    refute buffer.scheduled?
  end

  test "drain empties and clears scheduled flag" do
    buffer = DirtyBuffer.new(threshold: 100)
    {:schedule, buffer} = DirtyBuffer.enqueue(buffer, [:a, :b])

    {items, buffer} = DirtyBuffer.drain(buffer)
    assert Enum.sort(items) == [:a, :b]
    assert DirtyBuffer.empty?(buffer)
    refute buffer.scheduled?

    # Next enqueue arms a fresh timer.
    assert {:schedule, _} = DirtyBuffer.enqueue(buffer, [:c])
  end

  test "duplicates collapse via MapSet" do
    buffer = DirtyBuffer.new(threshold: 100)
    {:schedule, buffer} = DirtyBuffer.enqueue(buffer, [:a, :a, :b])
    assert DirtyBuffer.size(buffer) == 2
  end
end
