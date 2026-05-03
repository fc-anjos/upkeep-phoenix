defmodule Upkeep.SourceEctoTest do
  use Upkeep.DataCase, async: false

  defmodule Issue do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_test_issues" do
      field :project_id, :integer
      field :assignee_id, :integer
      field :status, :string
      field :title, :string
      field :position, :integer
    end
  end

  defmodule ProjectIssues do
    use Upkeep.Source, repo: Upkeep.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, user_id: user_id}) do
      from i in Issue,
        where:
          i.project_id == ^project_id and
            i.assignee_id == ^user_id and
            i.status == "open",
        order_by: [asc: i.position]
    end
  end

  defmodule BroadProjectIssues do
    use Upkeep.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, user_id: user_id}) do
      from i in Issue,
        where: i.project_id == ^project_id or i.assignee_id == ^user_id
    end
  end

  setup do
    Repo.query!("DROP TABLE IF EXISTS upkeep_source_ecto_test_issues")

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_issues (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      assignee_id INTEGER,
      status TEXT NOT NULL,
      title TEXT NOT NULL,
      position INTEGER NOT NULL
    )
    """)

    :ok
  end

  test "plain query/1 sources load through their configured repo" do
    Repo.insert!(issue(id: 1, project_id: 1, assignee_id: 9, title: "Mine", position: 2))
    Repo.insert!(issue(id: 2, project_id: 1, assignee_id: 10, title: "Other", position: 1))
    Repo.insert!(issue(id: 3, project_id: 1, assignee_id: 9, title: "First", position: 1))

    issues = ProjectIssues.load(%{project_id: 1, user_id: 9})

    assert Enum.map(issues, & &1.title) == ["First", "Mine"]
  end

  test "plain query/1 sources infer field-indexed interest keys from Ecto where clauses" do
    assert ProjectIssues.__upkeep_interest_keys__(%{project_id: 1, user_id: 9}) == [
             {:upkeep_change, :inserted, Issue, [assignee_id: 9, project_id: 1, status: "open"]},
             {:upkeep_change, :updated, Issue, [assignee_id: 9, project_id: 1, status: "open"]},
             {:upkeep_change, :deleted, Issue, [assignee_id: 9, project_id: 1, status: "open"]}
           ]
  end

  test "plain query/1 sources react to inserts, updates, and deletes entering or leaving the query" do
    params = %{project_id: 1, user_id: 9}

    assert ProjectIssues.reacts_to?(
             issue(project_id: 1, assignee_id: 9, status: "open") |> Upkeep.Change.inserted(),
             params
           )

    assert ProjectIssues.reacts_to?(
             issue(project_id: 1, assignee_id: 10, status: "open")
             |> Upkeep.Change.updated(from: issue(project_id: 1, assignee_id: 9, status: "open")),
             params
           )

    assert ProjectIssues.reacts_to?(
             issue(project_id: 1, assignee_id: 9, status: "archived")
             |> Upkeep.Change.updated(from: issue(project_id: 1, assignee_id: 9, status: "open")),
             params
           )

    refute ProjectIssues.reacts_to?(
             issue(project_id: 1, assignee_id: 10, status: "open") |> Upkeep.Change.updated(),
             params
           )

    refute ProjectIssues.reacts_to?(
             Upkeep.Change.changed(
               :issue_renamed,
               issue(project_id: 1, assignee_id: 9, status: "open")
             ),
             params
           )
  end

  test "unsupported query shapes fall back to broad schema invalidation" do
    assert BroadProjectIssues.__upkeep_interest_keys__(%{project_id: 1, user_id: 9}) == [
             {:upkeep_change, :inserted, Issue},
             {:upkeep_change, :updated, Issue},
             {:upkeep_change, :deleted, Issue}
           ]

    assert BroadProjectIssues.reacts_to?(
             issue(project_id: 2, assignee_id: 10, status: "open") |> Upkeep.Change.updated(),
             %{project_id: 1, user_id: 9}
           )
  end

  defp issue(attrs) do
    struct!(
      Issue,
      Keyword.merge(
        [id: 1, project_id: 1, assignee_id: 9, status: "open", title: "Issue", position: 1],
        attrs
      )
    )
  end
end
