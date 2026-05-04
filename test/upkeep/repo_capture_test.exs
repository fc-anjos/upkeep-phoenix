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

  test "repo insert capture refreshes Ecto query sources after mutate commits" do
    socket = watch_project(user_id: 9)

    assert {:ok, :created} =
             Upkeep.mutate(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9, title: "Created"))
               :created
             end)

    socket = assert_project_issues(socket, 9, ["Created"])
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

    mine = assert_project_issues(mine, 9, ["Moved"])
    theirs = assert_project_issues(theirs, 10, [])

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

    socket = assert_project_issues(socket, 9, [])
    assert socket.assigns.issues == []
  end

  test "repo capture discards notifications when mutate rolls back" do
    watch_project(user_id: 9)

    assert {:error, :cancelled} =
             Upkeep.mutate(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9))
               Repo.rollback(:cancelled)
             end)

    refute_receive {:dag_values, [{_, _}]}
    refute Repo.get(Issue, 1)
  end

  test "repo transaction capture flushes only after direct transaction commits" do
    watch_project(user_id: 9)

    assert {:ok, :created} =
             Repo.transaction(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9))
               :created
             end)

    assert_project_issues(9, ["Issue"])
  end

  test "repo transaction capture discards direct transaction rollbacks" do
    watch_project(user_id: 9)

    assert {:error, :cancelled} =
             Repo.transaction(fn ->
               Repo.insert!(issue(id: 1, assignee_id: 9))
               Repo.rollback(:cancelled)
             end)

    refute_receive {:dag_values, [{_, _}]}
    refute Repo.get(Issue, 1)
  end

  test "Ecto.Multi changeset operations are captured automatically" do
    watch_project(user_id: 9)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.insert(:issue, issue(id: 1, assignee_id: 9))

    assert {:ok, %{issue: %Issue{id: 1}}} = Upkeep.mutate(multi)

    assert_project_issues(9, ["Issue"])
  end

  test "insert_or_update capture chooses inserted or updated from schema state" do
    watch_project(user_id: 9)

    issue(id: 1, assignee_id: 9)
    |> Changeset.change()
    |> Repo.insert_or_update!()

    assert_project_issues(9, ["Issue"])

    Issue
    |> Repo.get!(1)
    |> Changeset.change(title: "Updated")
    |> Repo.insert_or_update!()

    assert_project_issues(9, ["Updated"])
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

    socket = assert_project_issues(socket, 9, ["Mine"])
    refute_receive {:dag_values, [{{ProjectIssues, %{project_id: 1, user_id: 10}}, _}]}
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

    socket = assert_project_issues(socket, 9, ["Partial"])
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

    socket = assert_project_issues(socket, 9, ["Table"])
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

    socket = assert_table_project_issues(socket, 9, ["Table source"])
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

    socket = assert_project_issues(socket, 9, ["Imported"])
    refute_receive {:dag_values, [{{ProjectIssues, %{project_id: 1, user_id: 10}}, _}]}
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

    socket = assert_table_project_issues(socket, 9, ["Imported table"])
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

    mine = assert_project_issues(mine, 9, ["Moved", "Mine"])
    theirs = assert_project_issues(theirs, 10, [])

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

    mine = assert_table_project_issues(mine, 9, ["Moved", "Mine"])
    theirs = assert_table_project_issues(theirs, 10, [])

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

    socket = assert_project_issues(socket, 9, ["Moved"])
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

    assert_project_issues(9, ["Moved"])
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

    socket = assert_project_issues(socket, 9, [])
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

    socket = assert_table_project_issues(socket, 9, [])
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

    refute_receive {:dag_values, [{_, _}]}
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

    assert_project_issues(9, ["Multi"])
  end

  test "bulk capture can be disabled" do
    watch_project(user_id: 9)

    Repo.insert_all(Issue, [issue_attrs(id: 1, assignee_id: 9)], upkeep: false)

    refute_receive {:dag_values, [{_, _}]}
  end

  test "repo capture can be disabled for setup writes" do
    watch_project(user_id: 9)

    Repo.insert!(issue(id: 1, assignee_id: 9), upkeep: false)

    refute_receive {:dag_values, [{_, _}]}
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

  defp assert_project_issues(user_id, titles) do
    source_id = {ProjectIssues, %{project_id: 1, user_id: user_id}}
    issues = receive_dag_value(source_id)
    assert Enum.map(issues, & &1.title) == titles
    issues
  end

  @dag_buffer_key {__MODULE__, :dag_buffer}

  defp receive_dag_value(source_id) do
    buffered = Process.get(@dag_buffer_key, [])

    case Enum.split_with(buffered, fn {id, _} -> id == source_id end) do
      {[{^source_id, value} | rest_match], remaining} ->
        Process.put(@dag_buffer_key, rest_match ++ remaining)
        value

      {[], _} ->
        drain_until(source_id)
    end
  end

  defp drain_until(source_id) do
    receive do
      {:dag_values, batch} ->
        case Enum.split_with(batch, fn {id, _} -> id == source_id end) do
          {[{^source_id, value} | rest_match], remaining} ->
            existing = Process.get(@dag_buffer_key, [])
            Process.put(@dag_buffer_key, existing ++ rest_match ++ remaining)
            value

          {[], _} ->
            existing = Process.get(@dag_buffer_key, [])
            Process.put(@dag_buffer_key, existing ++ batch)
            drain_until(source_id)
        end
    after
      1_000 -> flunk("did not receive :dag_values for #{inspect(source_id)}")
    end
  end

  defp assert_project_issues(socket, user_id, titles) do
    issues = assert_project_issues(user_id, titles)
    Live.apply_dag_value(socket, {ProjectIssues, %{project_id: 1, user_id: user_id}}, issues)
  end

  defp assert_table_project_issues(socket, user_id, titles) do
    source_id = {TableProjectIssues, %{project_id: 1, user_id: user_id}}
    issues = receive_dag_value(source_id)
    assert Enum.map(issues, & &1.title) == titles
    Live.apply_dag_value(socket, source_id, issues)
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
