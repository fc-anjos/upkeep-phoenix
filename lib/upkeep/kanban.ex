defmodule Upkeep.Kanban do
  @moduledoc """
  Small in-memory kanban domain used by Upkeep's LiveView reference app.
  """

  use Agent

  alias __MODULE__.{Activity, Column, Comment, Issue}

  @project_id 1
  @current_user_id 1

  defmodule Column do
    defstruct [:id, :project_id, :name, :position]
  end

  defmodule Issue do
    defstruct [:id, :project_id, :column_id, :assignee_id, :title, :status, :position]
  end

  defmodule Comment do
    defstruct [:id, :project_id, :issue_id, :body]
  end

  defmodule Activity do
    defstruct [:id, :project_id, :message]
  end

  def start_link(_opts) do
    Agent.start_link(fn -> seed_state() end, name: __MODULE__)
  end

  def reset! do
    Agent.update(__MODULE__, fn _state -> seed_state() end)
  end

  def project_id, do: @project_id
  def current_user_id, do: @current_user_id

  def board_columns(project_id) do
    Agent.get(__MODULE__, fn state ->
      state.columns
      |> Map.values()
      |> Enum.filter(&(&1.project_id == project_id))
      |> Enum.sort_by(& &1.position)
      |> Enum.map(fn column ->
        issues =
          state.issues
          |> Map.values()
          |> Enum.filter(
            &(&1.project_id == project_id and &1.column_id == column.id and &1.status == :open)
          )
          |> Enum.sort_by(& &1.position)

        Map.put(column, :issues, issues)
      end)
    end)
  end

  def project_activity(project_id) do
    Agent.get(__MODULE__, fn state ->
      state.activity
      |> Enum.filter(&(&1.project_id == project_id))
      |> Enum.sort_by(& &1.id, :desc)
      |> Enum.take(8)
    end)
  end

  def my_issues(project_id, user_id) do
    Agent.get(__MODULE__, fn state ->
      state.issues
      |> Map.values()
      |> Enum.filter(
        &(&1.project_id == project_id and &1.assignee_id == user_id and &1.status == :open)
      )
      |> Enum.sort_by(& &1.id)
    end)
  end

  def issue_comments(issue_id) do
    Agent.get(__MODULE__, fn state ->
      state.comments
      |> Map.values()
      |> Enum.filter(&(&1.issue_id == issue_id))
      |> Enum.sort_by(& &1.id)
    end)
  end

  def create_issue(project_id, title) do
    Upkeep.mutate(fn ->
      issue =
        Agent.get_and_update(__MODULE__, fn state ->
          id = state.next_id

          issue = %Issue{
            id: id,
            project_id: project_id,
            column_id: 1,
            assignee_id: nil,
            title: title,
            status: :open,
            position: id
          }

          {issue,
           state
           |> put_in([:issues, id], issue)
           |> add_activity(project_id, "Created #{title}")
           |> Map.update!(:next_id, &(&1 + 1))}
        end)

      Upkeep.changed(:issue_created, issue)
      issue
    end)
  end

  def move_issue(issue_id, column_id) do
    Upkeep.mutate(fn ->
      {old_issue, issue} =
        Agent.get_and_update(__MODULE__, fn state ->
          old_issue = Map.fetch!(state.issues, issue_id)
          issue = %{old_issue | column_id: column_id, position: state.next_id}

          {{old_issue, issue},
           state
           |> put_in([:issues, issue_id], issue)
           |> add_activity(issue.project_id, "Moved #{issue.title}")
           |> Map.update!(:next_id, &(&1 + 1))}
        end)

      Upkeep.changed(:issue_moved, issue, from: old_issue)
      issue
    end)
  end

  def assign_issue(issue_id, user_id) do
    Upkeep.mutate(fn ->
      {old_issue, issue} =
        Agent.get_and_update(__MODULE__, fn state ->
          old_issue = Map.fetch!(state.issues, issue_id)
          issue = %{old_issue | assignee_id: user_id}

          {{old_issue, issue},
           state
           |> put_in([:issues, issue_id], issue)
           |> add_activity(issue.project_id, "Assigned #{issue.title}")
           |> Map.update!(:next_id, &(&1 + 1))}
        end)

      Upkeep.changed(:issue_assigned, issue, from: old_issue)
      issue
    end)
  end

  def rename_issue(issue_id, title) do
    Upkeep.mutate(fn ->
      {old_issue, issue} =
        Agent.get_and_update(__MODULE__, fn state ->
          old_issue = Map.fetch!(state.issues, issue_id)
          issue = %{old_issue | title: title}

          {{old_issue, issue},
           state
           |> put_in([:issues, issue_id], issue)
           |> add_activity(issue.project_id, "Renamed #{old_issue.title} to #{issue.title}")
           |> Map.update!(:next_id, &(&1 + 1))}
        end)

      Upkeep.changed(:issue_renamed, issue, from: old_issue)
      issue
    end)
  end

  def add_comment(issue_id, body) do
    Upkeep.mutate(fn ->
      comment =
        Agent.get_and_update(__MODULE__, fn state ->
          issue = Map.fetch!(state.issues, issue_id)
          id = state.next_id
          comment = %Comment{id: id, project_id: issue.project_id, issue_id: issue_id, body: body}

          {comment,
           state
           |> put_in([:comments, id], comment)
           |> add_activity(issue.project_id, "Commented on #{issue.title}")
           |> Map.update!(:next_id, &(&1 + 1))}
        end)

      Upkeep.changed(:comment_added, comment)
      comment
    end)
  end

  def archive_issue(issue_id) do
    Upkeep.mutate(fn ->
      {old_issue, issue} =
        Agent.get_and_update(__MODULE__, fn state ->
          old_issue = Map.fetch!(state.issues, issue_id)
          issue = %{old_issue | status: :archived}

          {{old_issue, issue},
           state
           |> put_in([:issues, issue_id], issue)
           |> add_activity(issue.project_id, "Archived #{issue.title}")
           |> Map.update!(:next_id, &(&1 + 1))}
        end)

      Upkeep.changed(:issue_archived, issue, from: old_issue)
      issue
    end)
  end

  defp seed_state do
    columns = %{
      1 => %Column{id: 1, project_id: @project_id, name: "Backlog", position: 1},
      2 => %Column{id: 2, project_id: @project_id, name: "Doing", position: 2},
      3 => %Column{id: 3, project_id: @project_id, name: "Done", position: 3}
    }

    issues = %{
      10 => %Issue{
        id: 10,
        project_id: @project_id,
        column_id: 1,
        assignee_id: @current_user_id,
        title: "Shape source API",
        status: :open,
        position: 10
      },
      11 => %Issue{
        id: 11,
        project_id: @project_id,
        column_id: 2,
        assignee_id: nil,
        title: "Test durable fanout",
        status: :open,
        position: 11
      }
    }

    %{
      columns: columns,
      issues: issues,
      comments: %{
        20 => %Comment{
          id: 20,
          project_id: @project_id,
          issue_id: 10,
          body: "Keep the source contract boring."
        }
      },
      activity: [%Activity{id: 1, project_id: @project_id, message: "Seeded project board"}],
      next_id: 100
    }
  end

  defp add_activity(state, project_id, message) do
    activity = %Activity{id: state.next_id, project_id: project_id, message: message}
    %{state | activity: [activity | state.activity]}
  end
end
