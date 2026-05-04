defmodule Upkeep.Kanban do
  @moduledoc """
  Small Ecto-backed kanban domain used by Upkeep's LiveView reference app.
  """

  use GenServer

  import Ecto.Query

  alias Ecto.Changeset
  alias Upkeep.Kanban.{Activity, Column, Comment, Issue}
  alias Upkeep.Repo

  @project_id 1
  @current_user_id 1

  defmodule Column do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "kanban_columns" do
      field :project_id, :integer
      field :name, :string
      field :position, :integer
    end
  end

  defmodule Issue do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "kanban_issues" do
      field :project_id, :integer
      field :column_id, :integer
      field :assignee_id, :integer
      field :title, :string
      field :status, :string
      field :position, :integer
    end
  end

  defmodule Comment do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "kanban_comments" do
      field :project_id, :integer
      field :issue_id, :integer
      field :body, :string
    end
  end

  defmodule Activity do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "kanban_activity" do
      field :project_id, :integer
      field :message, :string
    end
  end

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    ensure_tables!()
    reset!()
    {:ok, nil}
  end

  def reset! do
    ensure_tables!()

    Repo.transaction(fn ->
      Repo.delete_all(Activity, upkeep: false)
      Repo.delete_all(Comment, upkeep: false)
      Repo.delete_all(Issue, upkeep: false)
      Repo.delete_all(Column, upkeep: false)

      Enum.each(seed_columns(), &Repo.insert!(&1, upkeep: false))
      Enum.each(seed_issues(), &Repo.insert!(&1, upkeep: false))
      Enum.each(seed_comments(), &Repo.insert!(&1, upkeep: false))

      Repo.insert!(%Activity{id: 1, project_id: @project_id, message: "Seeded project board"},
        upkeep: false
      )
    end)

    :ok
  end

  def project_id, do: @project_id
  def current_user_id, do: @current_user_id

  def board_columns(project_id) do
    columns =
      project_id
      |> columns_query()
      |> Repo.all()

    issues =
      project_id
      |> open_issues_query()
      |> Repo.all()
      |> Enum.group_by(& &1.column_id)

    Enum.map(columns, fn column ->
      Map.put(column, :issues, Map.get(issues, column.id, []))
    end)
  end

  def project_activity(project_id) do
    project_id
    |> activity_query()
    |> Repo.all()
  end

  def my_issues(project_id, user_id) do
    project_id
    |> my_issues_query(user_id)
    |> Repo.all()
  end

  def archived_issues(project_id) do
    project_id
    |> archived_issues_query()
    |> Repo.all()
  end

  def issue_comments(issue_id) do
    issue_id
    |> comments_query()
    |> Repo.all()
  end

  def columns_query(project_id) do
    from c in Column,
      where: c.project_id == ^project_id,
      order_by: [asc: c.position]
  end

  def open_issues_query(project_id) do
    from i in Issue,
      where: i.project_id == ^project_id and i.status == "open",
      order_by: [asc: i.position]
  end

  def board_columns_query(project_id) do
    from c in Column,
      join: i in Issue,
      on: i.project_id == c.project_id and i.column_id == c.id,
      where: c.project_id == ^project_id,
      where: i.project_id == ^project_id and i.status == "open",
      select: c
  end

  def activity_query(project_id) do
    from a in Activity,
      where: a.project_id == ^project_id,
      order_by: [desc: a.id],
      limit: 8
  end

  def my_issues_query(project_id, user_id) do
    from i in Issue,
      where:
        i.project_id == ^project_id and
          i.assignee_id == ^user_id and
          i.status == "open",
      order_by: [asc: i.id]
  end

  def archived_issues_query(project_id) do
    from i in Issue,
      where: i.project_id == ^project_id and i.status == "archived",
      order_by: [desc: i.position]
  end

  def comments_query(issue_id) do
    from c in Comment,
      where: c.issue_id == ^issue_id,
      order_by: [asc: c.id]
  end

  def create_issue(project_id, title) do
    Upkeep.mutate(fn ->
      id = next_id()

      issue =
        %Issue{
          id: id,
          project_id: project_id,
          column_id: 1,
          assignee_id: nil,
          title: title,
          status: "open",
          position: id
        }
        |> Repo.insert!()

      add_activity(project_id, "Created #{title}")
      issue
    end)
  end

  def move_issue(issue_id, column_id) do
    Upkeep.mutate(fn ->
      issue = Repo.get!(Issue, issue_id)

      issue =
        issue
        |> Changeset.change(column_id: column_id, position: next_id())
        |> Repo.update!()

      add_activity(issue.project_id, "Moved #{issue.title}")
      issue
    end)
  end

  def assign_issue(issue_id, user_id) do
    Upkeep.mutate(fn ->
      issue = Repo.get!(Issue, issue_id)

      issue =
        issue
        |> Changeset.change(assignee_id: user_id)
        |> Repo.update!()

      add_activity(issue.project_id, "Assigned #{issue.title}")
      issue
    end)
  end

  def rename_issue(issue_id, title) do
    Upkeep.mutate(fn ->
      issue = Repo.get!(Issue, issue_id)
      old_title = issue.title

      issue =
        issue
        |> Changeset.change(title: title)
        |> Repo.update!()

      add_activity(issue.project_id, "Renamed #{old_title} to #{issue.title}")
      issue
    end)
  end

  def add_comment(issue_id, body) do
    Upkeep.mutate(fn ->
      issue = Repo.get!(Issue, issue_id)

      comment =
        %Comment{id: next_id(), project_id: issue.project_id, issue_id: issue_id, body: body}
        |> Repo.insert!()

      add_activity(issue.project_id, "Commented on #{issue.title}")
      comment
    end)
  end

  def archive_issue(issue_id) do
    Upkeep.mutate(fn ->
      issue = Repo.get!(Issue, issue_id)

      issue =
        issue
        |> Changeset.change(status: "archived")
        |> Repo.update!()

      add_activity(issue.project_id, "Archived #{issue.title}")
      issue
    end)
  end

  def restore_issue(issue_id, column_id) do
    Upkeep.mutate(fn ->
      issue = Repo.get!(Issue, issue_id)

      issue =
        issue
        |> Changeset.change(column_id: column_id, status: "open", position: next_id())
        |> Repo.update!()

      add_activity(issue.project_id, "Restored #{issue.title}")
      issue
    end)
  end

  defp add_activity(project_id, message) do
    %Activity{id: next_id(), project_id: project_id, message: message}
    |> Repo.insert!()
  end

  defp next_id do
    [
      max_id(Column),
      max_id(Issue),
      max_id(Comment),
      max_id(Activity),
      99
    ]
    |> Enum.max()
    |> Kernel.+(1)
  end

  defp max_id(schema) do
    Repo.one(from r in schema, select: max(r.id)) || 0
  end

  defp seed_columns do
    [
      %Column{id: 1, project_id: @project_id, name: "Backlog", position: 1},
      %Column{id: 2, project_id: @project_id, name: "Doing", position: 2},
      %Column{id: 3, project_id: @project_id, name: "Done", position: 3}
    ]
  end

  defp seed_issues do
    [
      %Issue{
        id: 10,
        project_id: @project_id,
        column_id: 1,
        assignee_id: @current_user_id,
        title: "Shape source API",
        status: "open",
        position: 10
      },
      %Issue{
        id: 11,
        project_id: @project_id,
        column_id: 2,
        assignee_id: nil,
        title: "Test durable fanout",
        status: "open",
        position: 11
      }
    ]
  end

  defp seed_comments do
    [
      %Comment{
        id: 20,
        project_id: @project_id,
        issue_id: 10,
        body: "Keep the source contract boring."
      }
    ]
  end

  defp ensure_tables! do
    Repo.query!("""
    CREATE TABLE IF NOT EXISTS kanban_columns (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      position INTEGER NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE IF NOT EXISTS kanban_issues (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      column_id INTEGER NOT NULL,
      assignee_id INTEGER,
      title TEXT NOT NULL,
      status TEXT NOT NULL,
      position INTEGER NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE IF NOT EXISTS kanban_comments (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      issue_id INTEGER NOT NULL,
      body TEXT NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE IF NOT EXISTS kanban_activity (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      message TEXT NOT NULL
    )
    """)
  end
end
