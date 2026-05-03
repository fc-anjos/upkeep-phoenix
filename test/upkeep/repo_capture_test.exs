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

  defmodule TableProjectIssues do
    use Upkeep.Source, repo: Upkeep.Repo

    import Ecto.Query

    @table "upkeep_repo_capture_test_issues"

    def query(%{project_id: project_id, user_id: user_id}) do
      from i in @table,
        where:
          field(i, :project_id) == ^project_id and
            field(i, :assignee_id) == ^user_id and
            field(i, :status) == "open",
        order_by: [asc: field(i, :position)],
        select: %{
          id: field(i, :id),
          project_id: field(i, :project_id),
          assignee_id: field(i, :assignee_id),
          status: field(i, :status),
          title: field(i, :title),
          position: field(i, :position)
        }
    end
  end

  setup do
    Repo.query!("DROP TABLE IF EXISTS upkeep_repo_capture_test_imports")
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

    Repo.query!("""
    CREATE TABLE upkeep_repo_capture_test_imports (
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

  test "insert_all capture emits inserted changes from submitted entries" do
    socket = watch_project(user_id: 9)

    assert {:ok, {2, nil}} =
             Upkeep.mutate(fn ->
               Repo.insert_all(Issue, [
                 issue_attrs(id: 1, assignee_id: 9, title: "Mine"),
                 issue_attrs(id: 2, assignee_id: 10, title: "Other")
               ])
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}} =
                      change}

    refute_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 2}}}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Mine"]
  end

  test "insert_all capture merges partial returning rows with submitted entries" do
    socket = watch_project(user_id: 9)

    assert {:ok, {1, [%Issue{id: 1}]}} =
             Upkeep.mutate(fn ->
               Repo.insert_all(
                 Issue,
                 [issue_attrs(id: 1, assignee_id: 9, title: "Partial")],
                 returning: [:id]
               )
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :inserted,
                      schema: Issue,
                      record: %Issue{id: 1, project_id: 1, assignee_id: 9, status: "open"}
                    } = change}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Partial"]
  end

  test "insert_all capture supports schemaless table writes with a schema bridge" do
    socket = watch_project(user_id: 9)

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.insert_all(
                 "upkeep_repo_capture_test_issues",
                 [issue_attrs(id: 1, assignee_id: 9, title: "Table")],
                 upkeep: [schema: Issue]
               )
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}} =
                      change}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Table"]
  end

  test "insert_all capture supports schemaless sources and table-keyed changes" do
    socket = watch_table_project(user_id: 9)

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.insert_all("upkeep_repo_capture_test_issues", [
                 issue_attrs(id: 1, assignee_id: 9, title: "Table source")
               ])
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :inserted,
                      schema: "upkeep_repo_capture_test_issues",
                      record: %{id: 1, assignee_id: 9}
                    } = change}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Table source"]
  end

  test "insert_all capture supports schema-backed insert from query" do
    socket = watch_project(user_id: 9)

    Repo.insert_all(
      "upkeep_repo_capture_test_imports",
      [
        issue_attrs(id: 1, assignee_id: 9, title: "Imported"),
        issue_attrs(id: 2, assignee_id: 10, title: "Other import")
      ],
      upkeep: false
    )

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.insert_all(Issue, import_query(user_id: 9))
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}} =
                      change}

    refute_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 2}}}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Imported"]
  end

  test "insert_all capture supports schemaless insert from query with table-keyed changes" do
    socket = watch_table_project(user_id: 9)

    Repo.insert_all(
      "upkeep_repo_capture_test_imports",
      [issue_attrs(id: 1, assignee_id: 9, title: "Imported table")],
      upkeep: false
    )

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.insert_all("upkeep_repo_capture_test_issues", import_query(user_id: 9))
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :inserted,
                      schema: "upkeep_repo_capture_test_issues",
                      record: %{id: 1, title: "Imported table"}
                    } = change}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Imported table"]
  end

  test "update_all capture emits updated changes with before and after records" do
    Repo.insert!(issue(id: 1, assignee_id: 10, title: "Moved"), upkeep: false)
    Repo.insert!(issue(id: 2, assignee_id: 9, title: "Mine"), upkeep: false)

    mine = watch_project(user_id: 9)
    theirs = watch_project(user_id: 10)

    assert Enum.map(mine.assigns.issues, & &1.title) == ["Mine"]
    assert Enum.map(theirs.assigns.issues, & &1.title) == ["Moved"]

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.update_all(
                 from(i in Issue, where: i.project_id == 1 and i.assignee_id == 10),
                 set: [assignee_id: 9]
               )
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :updated,
                      schema: Issue,
                      record: %Issue{id: 1, assignee_id: 9},
                      from: %Issue{id: 1, assignee_id: 10}
                    } = change}

    mine = Live.refresh_matching(mine, change)
    theirs = Live.refresh_matching(theirs, change)

    assert Enum.map(mine.assigns.issues, & &1.title) == ["Moved", "Mine"]
    assert theirs.assigns.issues == []
  end

  test "update_all capture supports schemaless table queries" do
    Repo.insert!(issue(id: 1, assignee_id: 10, title: "Moved"), upkeep: false)
    Repo.insert!(issue(id: 2, assignee_id: 9, title: "Mine"), upkeep: false)

    mine = watch_table_project(user_id: 9)
    theirs = watch_table_project(user_id: 10)

    assert Enum.map(mine.assigns.issues, & &1.title) == ["Mine"]
    assert Enum.map(theirs.assigns.issues, & &1.title) == ["Moved"]

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.update_all(
                 from(i in "upkeep_repo_capture_test_issues",
                   where: field(i, :project_id) == 1 and field(i, :assignee_id) == 10
                 ),
                 set: [assignee_id: 9]
               )
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :updated,
                      schema: "upkeep_repo_capture_test_issues",
                      record: %{id: 1, assignee_id: 9},
                      from: %{id: 1, assignee_id: 10}
                    } = change}

    mine = Live.refresh_matching(mine, change)
    theirs = Live.refresh_matching(theirs, change)

    assert Enum.map(mine.assigns.issues, & &1.title) == ["Moved", "Mine"]
    assert theirs.assigns.issues == []
  end

  test "update_all capture supports schemaless table queries with a schema bridge" do
    Repo.insert!(issue(id: 1, assignee_id: 10, title: "Moved"), upkeep: false)
    socket = watch_project(user_id: 9)

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.update_all(
                 from(i in "upkeep_repo_capture_test_issues",
                   where: field(i, :project_id) == 1 and field(i, :assignee_id) == 10
                 ),
                 [set: [assignee_id: 9]],
                 upkeep: [schema: Issue]
               )
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :updated,
                      schema: Issue,
                      record: %Issue{id: 1, assignee_id: 9},
                      from: %Issue{id: 1, assignee_id: 10}
                    } = change}

    socket = Live.refresh_matching(socket, change)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Moved"]
  end

  test "bulk capture flushes after direct update_all commits" do
    Repo.insert!(issue(id: 1, assignee_id: 10, title: "Moved"), upkeep: false)
    watch_project(user_id: 9)

    assert {1, nil} =
             Repo.update_all(
               from(i in Issue, where: i.project_id == 1 and i.assignee_id == 10),
               set: [assignee_id: 9]
             )

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :updated,
                      schema: Issue,
                      record: %Issue{id: 1, assignee_id: 9},
                      from: %Issue{id: 1, assignee_id: 10}
                    }}
  end

  test "delete_all capture emits deleted changes from affected rows" do
    Repo.insert!(issue(id: 1, assignee_id: 9, title: "Deleted"), upkeep: false)
    Repo.insert!(issue(id: 2, assignee_id: 10, title: "Other"), upkeep: false)

    socket = watch_project(user_id: 9)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Deleted"]

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.delete_all(from(i in Issue, where: i.project_id == 1 and i.assignee_id == 9))
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :deleted, schema: Issue, record: %Issue{id: 1}} =
                      change}

    socket = Live.refresh_matching(socket, change)
    assert socket.assigns.issues == []
  end

  test "delete_all capture supports schemaless table queries" do
    Repo.insert!(issue(id: 1, assignee_id: 9, title: "Deleted"), upkeep: false)
    Repo.insert!(issue(id: 2, assignee_id: 10, title: "Other"), upkeep: false)

    socket = watch_table_project(user_id: 9)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Deleted"]

    assert {:ok, {1, nil}} =
             Upkeep.mutate(fn ->
               Repo.delete_all(
                 from(i in "upkeep_repo_capture_test_issues",
                   where: field(i, :project_id) == 1 and field(i, :assignee_id) == 9
                 )
               )
             end)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{
                      name: :deleted,
                      schema: "upkeep_repo_capture_test_issues",
                      record: %{id: 1}
                    } = change}

    socket = Live.refresh_matching(socket, change)
    assert socket.assigns.issues == []
  end

  test "bulk capture discards notifications when the surrounding mutation rolls back" do
    Repo.insert!(issue(id: 1, assignee_id: 10), upkeep: false)
    watch_project(user_id: 9)

    assert {:error, :cancelled} =
             Upkeep.mutate(fn ->
               Repo.update_all(
                 from(i in Issue, where: i.id == 1),
                 set: [assignee_id: 9]
               )

               Repo.rollback(:cancelled)
             end)

    refute_receive {:upkeep_event, %Upkeep.Change{name: :updated, schema: Issue}}
    assert %Issue{assignee_id: 10} = Repo.get!(Issue, 1)
  end

  test "Ecto.Multi bulk operations are captured automatically" do
    watch_project(user_id: 9)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.insert_all(:issues, Issue, [
        issue_attrs(id: 1, assignee_id: 9, title: "Multi")
      ])

    assert {:ok, %{issues: {1, nil}}} = Upkeep.mutate(multi)

    assert_receive {:upkeep_event,
                    %Upkeep.Change{name: :inserted, schema: Issue, record: %Issue{id: 1}}}
  end

  test "bulk capture can be disabled" do
    watch_project(user_id: 9)

    Repo.insert_all(Issue, [issue_attrs(id: 1, assignee_id: 9)], upkeep: false)

    refute_receive {:upkeep_event, %Upkeep.Change{name: :inserted, schema: Issue}}
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

  defp watch_table_project(opts) do
    user_id = Keyword.fetch!(opts, :user_id)

    %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    |> Live.watch(:issues, TableProjectIssues, project_id: 1, user_id: user_id)
  end

  defp issue(attrs) do
    struct!(
      Issue,
      issue_attrs(attrs)
    )
  end

  defp issue_attrs(attrs) do
    attrs =
      Keyword.merge(
        [id: 1, project_id: 1, assignee_id: 9, status: "open", title: "Issue", position: 1],
        attrs
      )

    Map.new(attrs)
  end

  defp import_query(opts) do
    user_id = Keyword.fetch!(opts, :user_id)

    from i in "upkeep_repo_capture_test_imports",
      where:
        field(i, :project_id) == 1 and
          field(i, :assignee_id) == ^user_id and
          field(i, :status) == "open",
      select: %{
        id: field(i, :id),
        project_id: field(i, :project_id),
        assignee_id: field(i, :assignee_id),
        status: field(i, :status),
        title: field(i, :title),
        position: field(i, :position)
      }
  end
end
