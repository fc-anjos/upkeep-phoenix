defmodule Upkeep.ChangeEqualityTest do
  @moduledoc """
  Guards the dedup contract for the sharded coordinator.

  Sharded coalescing collapses `==`-equal `Upkeep.Change` events in a flush
  window. That is sound iff two semantically-distinct mutations are never
  `==`. These tests assert the granularity properties dedup relies on, and
  document the acceptable collisions (two events with identical struct
  content — a no-op-shaped duplicate that is safe to drop under
  reload-trigger semantics).
  """
  use ExUnit.Case, async: true

  alias Upkeep.Change

  defmodule Row do
    defstruct [:id, :tenant_id, :name]
  end

  defp row(attrs), do: struct(Row, attrs)

  describe "distinguishes semantically distinct mutations" do
    test "different actions on the same record are not equal" do
      r = row(id: 1, name: "a")
      refute Change.inserted(r) == Change.updated(r)
      refute Change.updated(r) == Change.deleted(r)
    end

    test "different records are not equal" do
      a = row(id: 1, name: "a")
      b = row(id: 1, name: "b")
      refute Change.updated(a) == Change.updated(b)
    end

    test "different `from` values are not equal" do
      r = row(id: 1, name: "a")
      e1 = Change.updated(r, from: row(id: 1, name: "x"))
      e2 = Change.updated(r, from: row(id: 1, name: "y"))
      refute e1 == e2
    end

    test "different schemas are not equal even when fields match" do
      defmodule OtherRow do
        defstruct [:id, :tenant_id, :name]
      end

      a = row(id: 1, name: "a")
      b = struct(OtherRow, id: 1, name: "a")
      refute Change.updated(a) == Change.updated(b)
    end

    test "different semantic names are not equal" do
      refute Change.changed(:issue_moved, %{id: 1}) ==
               Change.changed(:issue_archived, %{id: 1})
    end

    test "differing meta makes events unequal" do
      r = row(id: 1, name: "a")
      e1 = Change.updated(r, meta: %{seq: 1})
      e2 = Change.updated(r, meta: %{seq: 2})
      refute e1 == e2
    end
  end

  describe "documented safe collisions (dedup may collapse these)" do
    test "two updates producing identical record+from collapse" do
      r = row(id: 1, name: "a")
      e1 = Change.updated(r, from: row(id: 1, name: "x"))
      e2 = Change.updated(r, from: row(id: 1, name: "x"))

      assert e1 == e2
      assert Enum.uniq([e1, e2]) == [e1]
    end

    test "two inserts of the same record collapse" do
      r = row(id: 1, name: "a")
      assert Change.inserted(r) == Change.inserted(r)
    end
  end
end
