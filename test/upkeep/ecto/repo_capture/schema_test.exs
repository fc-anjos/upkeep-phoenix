defmodule Upkeep.Ecto.RepoCapture.SchemaTest do
  use Upkeep.TestSupport.DataCase, async: false

  alias Upkeep.Ecto.RepoCapture.Schema

  defmodule Issue do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_repo_capture_test_issues" do
      field :project_id, :integer
      field :assignee_id, :integer
      field :status, :string
      field :title, :string
      field :position, :integer
    end
  end

  defmodule UnsupportedRepo do
    def __adapter__, do: UnsupportedAdapter
  end

  defmodule FailingMetadataRepo do
    def __adapter__, do: Ecto.Adapters.SQLite3
    def query!(_sql, _params), do: raise("metadata failed")
  end

  test "capture query returns a tagged schema-backed row shape" do
    assert {:ok, %Ecto.Query{} = query} =
             Schema.capture_query(
               Repo,
               from(i in "upkeep_repo_capture_test_issues", select: %{id: field(i, :id)}),
               Issue
             )

    assert {_, Issue} = query.from.source
    assert query.select
  end

  test "capture query returns a tagged schemaless row shape" do
    assert {:ok, %Ecto.Query{} = query} =
             Schema.capture_query(
               Repo,
               from(i in "upkeep_repo_capture_test_issues"),
               "upkeep_repo_capture_test_issues"
             )

    assert query.select
  end

  test "capture query returns data for row-shape deopts" do
    query = from(i in "upkeep_repo_capture_test_issues")

    assert {:deopt, :no_schema} = Schema.capture_query(Repo, query, nil)

    assert {:deopt, :no_table_fields} =
             Schema.capture_query(Repo, query, "upkeep_repo_capture_test_missing")

    assert {:deopt, :unsupported_adapter} =
             Schema.capture_query(UnsupportedRepo, query, "upkeep_repo_capture_test_issues")

    assert {:deopt, :table_metadata_failed} =
             Schema.capture_query(FailingMetadataRepo, query, "upkeep_repo_capture_test_issues")
  end
end
