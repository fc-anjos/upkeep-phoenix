defmodule Upkeep.BindingsTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Upkeep.Change
  alias Upkeep.Ecto.Source.QueryDeps
  alias Upkeep.InvalidationSurface

  defmodule Issue do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "bindings_test_issues" do
      field :project_id, :integer
      field :state, Ecto.Enum, values: [:open, :closed, :blocked]
      field :uid, Ecto.UUID
      field :title, :string
    end
  end

  defmodule Column do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "bindings_test_columns" do
      field :project_id, :integer
    end
  end

  describe "InvalidationSurface.canonical_value/3" do
    test "casts string params on integer columns to integers" do
      assert InvalidationSurface.canonical_value(Issue, :project_id, "7") == 7
      assert InvalidationSurface.canonical_value(Issue, :project_id, 7) == 7
    end

    test "casts Ecto.Enum dumped and loaded values to the same atom" do
      assert InvalidationSurface.canonical_value(Issue, :state, "open") == :open
      assert InvalidationSurface.canonical_value(Issue, :state, :open) == :open
    end

    test "casts dumped binary UUIDs and string UUIDs to the same string" do
      uuid = "11111111-1111-1111-1111-111111111111"
      {:ok, dumped} = Ecto.UUID.dump(uuid)

      assert InvalidationSurface.canonical_value(Issue, :uid, uuid) == uuid
      assert InvalidationSurface.canonical_value(Issue, :uid, dumped) == uuid
    end

    test "falls back to the raw value for schemaless tables" do
      assert InvalidationSurface.canonical_value("bindings_test_issues", :project_id, "7") == "7"
    end

    test "falls back to the raw value for unknown fields and uncastable values" do
      assert InvalidationSurface.canonical_value(Issue, :missing, "7") == "7"
      assert InvalidationSurface.canonical_value(Issue, :state, "bogus") == "bogus"
      assert InvalidationSurface.canonical_value(nil, :project_id, "7") == "7"
    end
  end

  describe "value-type normalization in surface keys" do
    test "string params build integer-canonical keys" do
      query = from i in Issue, where: i.project_id == ^"7"

      keys = QueryDeps.keys(query)

      assert {:upkeep_change, :inserted, Issue, [project_id: 7]} in keys
      refute {:upkeep_change, :inserted, Issue, [project_id: "7"]} in keys
    end

    test "matching normalizes dumped enum record values" do
      query = from i in Issue, where: i.project_id == ^1 and i.state == :open

      deps = QueryDeps.from_query(query)

      dumped =
        Change.changed(:inserted, %{project_id: 1, state: "open"},
          schema: Issue,
          record: %{project_id: 1, state: "open"}
        )

      assert QueryDeps.matches_change?(deps, dumped)

      mismatch = %{dumped | record: %{project_id: 1, state: "closed"}}
      refute QueryDeps.matches_change?(deps, mismatch)
    end
  end

  describe "union/combination and CTE traversal" do
    test "union arms contribute their inner schema deps" do
      arm = from c in Column, where: c.project_id == ^1, select: c.id

      query =
        from(i in Issue, where: i.project_id == ^1, select: i.id)
        |> union(^arm)

      keys = QueryDeps.keys(query)

      assert {:upkeep_change, :inserted, Column, [project_id: 1]} in keys
      assert {:upkeep_change, :inserted, Issue, [project_id: 1]} in keys
    end

    test "union_all arms contribute their inner schema deps" do
      arm = from c in Column, where: c.project_id == ^1, select: c.id

      query =
        from(i in Issue, where: i.project_id == ^1, select: i.id)
        |> union_all(^arm)

      keys = QueryDeps.keys(query)

      assert {:upkeep_change, :inserted, Column, [project_id: 1]} in keys
    end

    test "CTE arms contribute their inner schema deps" do
      cte = from c in Column, where: c.project_id == ^1, select: %{id: c.id}

      query =
        from(i in Issue, where: i.project_id == ^1)
        |> with_cte("cols", as: ^cte)

      keys = QueryDeps.keys(query)

      assert {:upkeep_change, :inserted, Column, [project_id: 1]} in keys
      assert {:upkeep_change, :inserted, Issue, [project_id: 1]} in keys
    end
  end
end
