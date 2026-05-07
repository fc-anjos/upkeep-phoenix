defmodule Upkeep.Ecto.RepoCapture.InsertAllRecordsTest do
  use ExUnit.Case, async: true

  alias Upkeep.Ecto.RepoCapture.InsertAllRecords

  defmodule Issue do
    defstruct [:id, :project_id, :assignee_id, :status, :title, :position]
  end

  test "materialize merges partial returning rows with submitted entries" do
    entries = InsertAllRecords.submitted_entries([issue_attrs(id: 1, title: "Submitted")], Issue)

    assert {:ok, [%Issue{id: 1, title: "Submitted", assignee_id: 9}]} =
             InsertAllRecords.materialize({1, [%Issue{id: 1}]}, Issue, entries)
  end

  test "materialize returns explicit deopts" do
    assert {:deopt, :no_schema} =
             InsertAllRecords.materialize({1, nil}, nil, {:ok, [issue_attrs(id: 1)]})

    assert {:deopt, :unmaterializable_entries} =
             InsertAllRecords.submitted_entries([:not_a_record], Issue)

    assert {:deopt, :no_returning_records} =
             InsertAllRecords.materialize({1, nil}, Issue, {:deopt, :unmaterializable_entries})
  end

  defp issue_attrs(attrs) do
    attrs =
      Keyword.merge(
        [id: 1, project_id: 1, assignee_id: 9, status: "open", title: "Issue", position: 1],
        attrs
      )

    Map.new(attrs)
  end
end
