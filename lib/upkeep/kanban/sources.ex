defmodule Upkeep.Kanban.Sources.BoardColumns do
  @moduledoc false

  use Upkeep.Source

  alias Upkeep.Kanban
  alias Upkeep.Kanban.{Column, Comment, Issue}

  query(fn s -> Kanban.board_columns(s.project_id) end)

  invalidated_by(Issue, :issue_created, on: :project_id)
  invalidated_by(Issue, :issue_moved, on: :project_id)
  invalidated_by(Issue, :issue_archived, on: :project_id)
  invalidated_by(Column, :updated, on: :project_id)
  invalidated_by(Comment, :comment_added, on: :project_id)
end

defmodule Upkeep.Kanban.Sources.ProjectActivity do
  @moduledoc false

  use Upkeep.Source

  alias Upkeep.Kanban

  query(fn s -> Kanban.project_activity(s.project_id) end)

  invalidated_by(:issue_created, on: :project_id)
  invalidated_by(:issue_moved, on: :project_id)
  invalidated_by(:issue_assigned, on: :project_id)
  invalidated_by(:issue_archived, on: :project_id)
  invalidated_by(:comment_added, on: :project_id)
end

defmodule Upkeep.Kanban.Sources.MyIssues do
  @moduledoc false

  use Upkeep.Source

  alias Upkeep.Kanban
  alias Upkeep.Kanban.Issue

  query(fn s -> Kanban.my_issues(s.project_id, s.user_id) end)

  invalidated_by(Issue, :issue_created,
    on: [:project_id, :assignee_id],
    as: [:project_id, :user_id]
  )

  reacts_to(Issue, :issue_assigned, fn change, s ->
    change.record.project_id == s.project_id and
      (Upkeep.Change.old(change, :assignee_id) == s.user_id or
         Upkeep.Change.new(change, :assignee_id) == s.user_id)
  end)

  reacts_to(Issue, :issue_archived, fn change, s ->
    change.record.project_id == s.project_id and
      (Upkeep.Change.old(change, :assignee_id) == s.user_id or
         Upkeep.Change.new(change, :assignee_id) == s.user_id)
  end)
end

defmodule Upkeep.Kanban.Sources.IssueComments do
  @moduledoc false

  use Upkeep.Source

  alias Upkeep.Kanban
  alias Upkeep.Kanban.Comment

  query(fn s -> Kanban.issue_comments(s.issue_id) end)

  invalidated_by(Comment, :comment_added, on: :issue_id)
end
