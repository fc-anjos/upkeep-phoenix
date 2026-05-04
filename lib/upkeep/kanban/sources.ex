defmodule Upkeep.Kanban.Sources.BoardColumns do
  @moduledoc false

  use Upkeep.Source, repo: Upkeep.Repo

  alias Upkeep.Kanban

  def load(s) do
    columns = Upkeep.read(Kanban.columns_query(s.project_id))

    issues =
      s.project_id
      |> Kanban.open_issues_query()
      |> Upkeep.read()
      |> Enum.group_by(& &1.column_id)

    Enum.map(columns, fn column ->
      Map.put(column, :issues, Map.get(issues, column.id, []))
    end)
  end
end

defmodule Upkeep.Kanban.Sources.ProjectActivity do
  @moduledoc false

  use Upkeep.Source, repo: Upkeep.Repo

  alias Upkeep.Kanban

  def query(s), do: Kanban.activity_query(s.project_id)
end

defmodule Upkeep.Kanban.Sources.MyIssues do
  @moduledoc false

  use Upkeep.Source, repo: Upkeep.Repo

  alias Upkeep.Kanban

  def query(s), do: Kanban.my_issues_query(s.project_id, s.user_id)
end

defmodule Upkeep.Kanban.Sources.ArchivedIssues do
  @moduledoc false

  use Upkeep.Source, repo: Upkeep.Repo

  alias Upkeep.Kanban

  def query(s), do: Kanban.archived_issues_query(s.project_id)
end

defmodule Upkeep.Kanban.Sources.IssueComments do
  @moduledoc false

  use Upkeep.Source, repo: Upkeep.Repo

  alias Upkeep.Kanban

  def query(s), do: Kanban.comments_query(s.issue_id)
end
