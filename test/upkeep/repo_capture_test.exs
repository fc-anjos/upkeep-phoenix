defmodule Upkeep.RepoCaptureTest do
  use Upkeep.DataCase, async: false

  alias Ecto.Changeset
  alias Upkeep.Live

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

  defmodule ProjectIssues do
    use Upkeep.Source, repo: Upkeep.Repo

    import Ecto.Query

    alias Upkeep.RepoCaptureTest.Issue

    def query(%{project_id: project_id, user_id: user_id}) do
      from i in Issue,
        where:
          i.project_id == ^project_id and
            i.assignee_id == ^user_id and
            i.status == "open",
        order_by: [asc: i.position]
    end
  end

  setup do
    Repo.query!("DROP TABLE IF EXISTS upkeep_repo_capture_test_issues")

    Repo.query!("""
    CREATE TABLE upkeep_repo_capture_test_issues (
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

  test "repo insert capture refreshes Ecto query sources after mutate commits" do
    socket = watch_project(user_id: 9)

    assert {:ok, :created} =
             Upkeep.mutate(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9, title: "Created"))
               :created
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}} =
                      change}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Created"]
  end

  test "repo update capture includes old and new records so sources refresh on enter and leave" do
    Repo.insert!(issue(id: 1, assignee_id: 10, title: "Moved"), upkeep: false)

    mine = watch_project(user_id: 9)
    theirs = watch_project(user_id: 10)

    assert mine.assigns.issues == []
    assert Enum.map(theirs.assigns.issues, & &1.title) == ["Moved"]

    issue =
      Issue
      |> Repo.get!(1)
      |> Changeset.change(assignee_id: 9)

    assert {:ok, %Issue{assignee_id: 9}} = Upkeep.mutate(fn -> Repo.update!(issue) end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :updated,
                      schema: Issue,
                      record: %Issue{assignee_id: 9},
                      from: %Issue{assignee_id: 10}
                    } = change}

    mine = Live.refresh_matching(mine, change)
    theirs = Live.refresh_matching(theirs, change)

    assert Enum.map(mine.assigns.issues, & &1.title) == ["Moved"]
    assert theirs.assigns.issues == []
  end

  test "repo delete capture refreshes sources that contained the deleted record" do
    Repo.insert!(issue(id: 1, assignee_id: 9, title: "Deleted"), upkeep: false)
    socket = watch_project(user_id: 9)

    assert Enum.map(socket.assigns.issues, & &1.title) == ["Deleted"]

    assert {:ok, %Issue{id: 1}} =
             Upkeep.mutate(fn ->
               Issue
               |> Repo.get!(1)
               |> Repo.delete!()
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :deleted, schema: Issue, record: %Issue{id: 1}} =
                      change}

    socket = Live.refresh_matching(socket, change)
    assert socket.assigns.issues == []
  end

  test "repo capture discards notifications when mutate rolls back" do
    watch_project(user_id: 9)

    assert {:error, :cancelled} =
             Upkeep.mutate(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9))
               Repo.rollback(:cancelled)
             end)

    refute_receive {:upkeep_event, %Upkeep.Change{name: :inserted, schema: Issue}}
    refute Repo.get(Issue, 1)
  end

  test "repo transaction capture flushes only after direct transaction commits" do
    watch_project(user_id: 9)

    assert {:ok, :created} =
             Repo.transaction(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9))
               :created
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}}}
  end

  test "repo transaction capture discards direct transaction rollbacks" do
    watch_project(user_id: 9)

    assert {:error, :cancelled} =
             Repo.transaction(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9))
               Repo.rollback(:cancelled)
             end)

    refute_receive {:upkeep_event, %Upkeep.Change{name: :inserted, schema: Issue}}
    refute Repo.get(Issue, 1)
  end

  test "Ecto.Multi changeset operations are captured automatically" do
    watch_project(user_id: 9)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.insert(:issue, issue(id: 1, assignee_id: 9))

    assert {:ok, %{issue: %Issue{id: 1}}} = Upkeep.mutate(multi)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}}}
  end

  test "insert_or_update capture chooses inserted or updated from schema state" do
    watch_project(user_id: 9)

    issue(id: 1, assignee_id: 9)
    |> Changeset.change()
    |> Repo.insert_or_update!()

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}}}

    Issue
    |> Repo.get!(1)
    |> Changeset.change(title: "Updated")
    |> Repo.insert_or_update!()

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :updated,
                      schema: Issue,
                      record: %Issue{title: "Updated"},
                      from: %Issue{title: "Issue"}
                    }}
  end

  test "repo capture can be disabled for setup writes" do
    watch_project(user_id: 9)

    Repo.insert!(issue(id: 1, assignee_id: 9), upkeep: false)

    refute_receive {:upkeep_event, %Upkeep.Change{name: :inserted, schema: Issue}}
  end

  defp watch_project(opts) do
    user_id = Keyword.fetch!(opts, :user_id)

    %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    |> Live.watch(:issues, ProjectIssues, project_id: 1, user_id: user_id)
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
