defmodule Upkeep.SourceEctoTest do
  use Upkeep.DataCase, async: false

  alias Upkeep.Live

  defmodule Column do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_test_columns" do
      field :project_id, :integer
      field :name, :string
      field :position, :integer
    end
  end

  defmodule Tag do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_test_tags" do
      field :name, :string
    end
  end

  defmodule IssueTag do
    use Ecto.Schema

    @primary_key false
    schema "upkeep_source_ecto_test_issue_tags" do
      field :issue_id, :integer
      field :tag_id, :integer
    end
  end

  defmodule Issue do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_test_issues" do
      field :project_id, :integer
      field :column_id, :integer
      field :assignee_id, :integer
      field :status, :string
      field :title, :string
      field :position, :integer

      belongs_to :column, Upkeep.SourceEctoTest.Column, define_field: false
      has_many :comments, Upkeep.SourceEctoTest.Comment

      many_to_many :tags, Upkeep.SourceEctoTest.Tag,
        join_through: Upkeep.SourceEctoTest.IssueTag,
        join_keys: [issue_id: :id, tag_id: :id]
    end
  end

  defmodule Comment do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_test_comments" do
      field :project_id, :integer
      field :issue_id, :integer
      field :body, :string
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

  defmodule JoinedIssueCards do
    use Upkeep.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.{Column, Issue}

    def query(%{project_id: project_id, statuses: statuses}) do
      from i in Issue,
        join: c in Column,
        on: c.id == i.column_id,
        where:
          i.project_id == ^project_id and
            c.project_id == ^project_id and
            i.status in ^statuses,
        order_by: [asc: c.position, asc: i.position],
        select: %{id: i.id, title: i.title, column: c.name}
    end
  end

  defmodule AssocJoinedIssueCards do
    use Upkeep.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id}) do
      from i in Issue,
        join: c in assoc(i, :column),
        where: i.project_id == ^project_id and c.project_id == ^project_id,
        select: {i.id, c.name}
    end
  end

  defmodule JoinedColumnProjection do
    use Upkeep.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.{Column, Issue}

    def query(%{project_id: project_id}) do
      from i in Issue,
        join: c in Column,
        on: c.id == i.column_id,
        where: i.project_id == ^project_id,
        select: %{title: i.title, column: c.name}
    end
  end

  defmodule DynamicIssues do
    use Upkeep.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, statuses: statuses}) do
      filters = dynamic([i], i.project_id == ^project_id and i.status in ^statuses)

      from i in Issue,
        where: ^filters
    end
  end

  defmodule CommentedIssues do
    use Upkeep.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.{Comment, Issue}

    def query(%{project_id: project_id}) do
      commented_issue_ids =
        from c in Comment,
          where: c.project_id == ^project_id,
          select: c.issue_id

      from i in Issue,
        where: i.project_id == ^project_id and i.id in subquery(commented_issue_ids)
    end
  end

  defmodule FragmentIssues do
    use Upkeep.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, term: term}) do
      from i in Issue,
        where: i.project_id == ^project_id and fragment("lower(?)", i.title) == ^term
    end
  end

  defmodule PreloadedProjectIssues do
    use Upkeep.Source, repo: Upkeep.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id}) do
      from i in Issue,
        where: i.project_id == ^project_id,
        order_by: [asc: i.position],
        preload: [:column, :comments, :tags]
    end
  end

  defmodule QueryPreloadedProjectIssues do
    use Upkeep.Source, repo: Upkeep.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.{Comment, Issue}

    def query(%{project_id: project_id}) do
      visible_comments =
        from c in Comment,
          where: c.body == "visible"

      from i in Issue,
        where: i.project_id == ^project_id,
        preload: [comments: ^visible_comments]
    end
  end

  defmodule SchemalessProjectIssues do
    use Upkeep.Source

    import Ecto.Query

    @table "upkeep_source_ecto_test_issues"

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

  defmodule BoardViewModel do
    use Upkeep.Source, repo: Upkeep.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.{Column, Issue}

    def load(%{project_id: project_id}) do
      columns =
        Upkeep.read(
          from c in Column,
            where: c.project_id == ^project_id,
            order_by: [asc: c.position]
        )

      issues =
        Upkeep.read(
          from i in Issue,
            where: i.project_id == ^project_id and i.status == "open",
            order_by: [asc: i.position]
        )
        |> Enum.group_by(& &1.column_id)

      Enum.map(columns, fn column ->
        %{column: column.name, issues: Map.get(issues, column.id, [])}
      end)
    end
  end

  setup do
    Repo.query!("DROP TABLE IF EXISTS upkeep_source_ecto_test_issue_tags")
    Repo.query!("DROP TABLE IF EXISTS upkeep_source_ecto_test_tags")
    Repo.query!("DROP TABLE IF EXISTS upkeep_source_ecto_test_comments")
    Repo.query!("DROP TABLE IF EXISTS upkeep_source_ecto_test_columns")
    Repo.query!("DROP TABLE IF EXISTS upkeep_source_ecto_test_issues")

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_issues (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      column_id INTEGER,
      assignee_id INTEGER,
      status TEXT NOT NULL,
      title TEXT NOT NULL,
      position INTEGER NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_columns (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      position INTEGER NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_comments (
      id INTEGER PRIMARY KEY,
      project_id INTEGER NOT NULL,
      issue_id INTEGER NOT NULL,
      body TEXT NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_tags (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE upkeep_source_ecto_test_issue_tags (
      issue_id INTEGER NOT NULL,
      tag_id INTEGER NOT NULL
    )
    """)

    :ok
  end

  test "plain query/1 sources load through their configured repo" do
    Repo.insert!(issue(id: 1, project_id: 1, assignee_id: 9, title: "Mine", position: 2))
    Repo.insert!(issue(id: 2, project_id: 1, assignee_id: 10, title: "Other", position: 1))
    Repo.insert!(issue(id: 3, project_id: 1, assignee_id: 9, title: "First", position: 1))

    {issues, _deps} = Upkeep.Source.load(ProjectIssues, %{project_id: 1, user_id: 9})

    assert Enum.map(issues, & &1.title) == ["First", "Mine"]
  end

  test "custom load/1 tracks each Upkeep.read query as the reactive surface" do
    Repo.insert!(%Column{id: 1, project_id: 1, name: "Backlog", position: 1})
    Repo.insert!(issue(id: 1, project_id: 1, column_id: 1, title: "Open", position: 1))
    Repo.insert!(issue(id: 2, project_id: 1, column_id: 1, status: "archived", title: "Archived"))

    {board, deps} = Upkeep.Source.load(BoardViewModel, %{project_id: 1})

    assert [%{column: "Backlog", issues: [%Issue{title: "Open"}]}] = board

    interest_keys = Upkeep.Source.deps_interest_keys(deps)

    assert {:upkeep_change, :inserted, Column, [project_id: 1]} in interest_keys
    assert {:upkeep_change, :updated, Issue, [project_id: 1, status: "open"]} in interest_keys

    assert Upkeep.Source.deps_react_to?(
             deps,
             issue(project_id: 1, column_id: 1, status: "open") |> Upkeep.Change.inserted()
           )

    refute Upkeep.Source.deps_react_to?(
             deps,
             issue(project_id: 1, column_id: 1, status: "archived") |> Upkeep.Change.inserted()
           )
  end

  test "plain query/1 sources infer field-indexed interest keys from Ecto where clauses" do
    assert ProjectIssues.__upkeep_interest_keys__(%{project_id: 1, user_id: 9}) == [
             {:upkeep_change, :inserted, Issue, [assignee_id: 9, project_id: 1, status: "open"]},
             {:upkeep_change, :updated, Issue, [assignee_id: 9, project_id: 1, status: "open"]},
             {:upkeep_change, :deleted, Issue, [assignee_id: 9, project_id: 1, status: "open"]}
           ]
  end

  test "coverage classifies field-indexed query deps as precise" do
    Repo.insert!(issue(id: 1, project_id: 1, assignee_id: 9, title: "Mine", position: 1))

    coverage = Upkeep.Test.assert_source_reactive!(ProjectIssues, %{project_id: 1, user_id: 9})

    assert coverage.unknown == []
    assert %{schema: Issue, fields: [:assignee_id, :project_id, :status]} in coverage.precise
    assert coverage.broad == []
  end

  test "plain query/1 sources refresh after captured repo inserts without explicit reactors" do
    Repo.insert!(issue(id: 1, project_id: 1, assignee_id: 9, title: "Before", position: 1),
      upkeep: false
    )

    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1, user_id: 9)

    assert Enum.map(socket.assigns.issues, & &1.title) == ["Before"]

    {:ok, %Issue{}} =
      Upkeep.mutate(fn ->
        Repo.insert!(issue(id: 2, project_id: 1, assignee_id: 9, title: "After", position: 2))
      end)

    :ok = Upkeep.Coordinator.Graph.drain()

    source_id = {ProjectIssues, %{project_id: 1, user_id: 9}}

    assert_receive {:dag_values, [{^source_id, issues}]}, 1_000
    assert Enum.map(issues, & &1.title) == ["Before", "After"]

    socket = Live.apply_dag_value(socket, source_id, issues)
    assert Enum.map(socket.assigns.issues, & &1.title) == ["Before", "After"]
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

  test "joined queries infer dependencies for every schema with equality filters" do
    params = %{project_id: 1, statuses: ["open", "blocked"]}

    assert JoinedIssueCards.__upkeep_interest_keys__(params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Column, [project_id: 1]},
               {:upkeep_change, :updated, Column, [project_id: 1]},
               {:upkeep_change, :deleted, Column, [project_id: 1]},
               {:upkeep_change, :inserted, Issue, [project_id: 1, status: "blocked"]},
               {:upkeep_change, :inserted, Issue, [project_id: 1, status: "open"]},
               {:upkeep_change, :updated, Issue, [project_id: 1, status: "blocked"]},
               {:upkeep_change, :updated, Issue, [project_id: 1, status: "open"]},
               {:upkeep_change, :deleted, Issue, [project_id: 1, status: "blocked"]},
               {:upkeep_change, :deleted, Issue, [project_id: 1, status: "open"]}
             ]
             |> sort_terms()

    assert JoinedIssueCards.reacts_to?(
             issue(project_id: 1, status: "blocked") |> Upkeep.Change.updated(),
             params
           )

    refute JoinedIssueCards.reacts_to?(
             issue(project_id: 1, status: "closed") |> Upkeep.Change.updated(),
             params
           )

    assert JoinedIssueCards.reacts_to?(
             column(project_id: 1) |> Upkeep.Change.updated(),
             params
           )

    refute JoinedIssueCards.reacts_to?(
             column(project_id: 2) |> Upkeep.Change.updated(),
             params
           )
  end

  test "assoc joins infer the related schema from Ecto association metadata" do
    params = %{project_id: 1}

    assert AssocJoinedIssueCards.__upkeep_interest_keys__(params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Column, [project_id: 1]},
               {:upkeep_change, :updated, Column, [project_id: 1]},
               {:upkeep_change, :deleted, Column, [project_id: 1]},
               {:upkeep_change, :inserted, Issue, [project_id: 1]},
               {:upkeep_change, :updated, Issue, [project_id: 1]},
               {:upkeep_change, :deleted, Issue, [project_id: 1]}
             ]
             |> sort_terms()
  end

  test "joined projections without equality filters fall back only for that schema" do
    params = %{project_id: 1}

    assert JoinedColumnProjection.__upkeep_interest_keys__(params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Column},
               {:upkeep_change, :updated, Column},
               {:upkeep_change, :deleted, Column},
               {:upkeep_change, :inserted, Issue, [project_id: 1]},
               {:upkeep_change, :updated, Issue, [project_id: 1]},
               {:upkeep_change, :deleted, Issue, [project_id: 1]}
             ]
             |> sort_terms()
  end

  test "dynamic filters and in-list filters are treated as precise memberships" do
    params = %{project_id: 1, statuses: ["open", "blocked"]}

    assert DynamicIssues.__upkeep_interest_keys__(params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Issue, [project_id: 1, status: "blocked"]},
               {:upkeep_change, :inserted, Issue, [project_id: 1, status: "open"]},
               {:upkeep_change, :updated, Issue, [project_id: 1, status: "blocked"]},
               {:upkeep_change, :updated, Issue, [project_id: 1, status: "open"]},
               {:upkeep_change, :deleted, Issue, [project_id: 1, status: "blocked"]},
               {:upkeep_change, :deleted, Issue, [project_id: 1, status: "open"]}
             ]
             |> sort_terms()

    assert DynamicIssues.reacts_to?(
             issue(project_id: 1, status: "open") |> Upkeep.Change.updated(),
             params
           )

    refute DynamicIssues.reacts_to?(
             issue(project_id: 1, status: "closed") |> Upkeep.Change.updated(),
             params
           )
  end

  test "subqueries add dependencies for their inner schemas" do
    params = %{project_id: 1}

    assert CommentedIssues.__upkeep_interest_keys__(params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Comment, [project_id: 1]},
               {:upkeep_change, :updated, Comment, [project_id: 1]},
               {:upkeep_change, :deleted, Comment, [project_id: 1]},
               {:upkeep_change, :inserted, Issue, [project_id: 1]},
               {:upkeep_change, :updated, Issue, [project_id: 1]},
               {:upkeep_change, :deleted, Issue, [project_id: 1]}
             ]
             |> sort_terms()

    assert CommentedIssues.reacts_to?(
             comment(project_id: 1) |> Upkeep.Change.inserted(),
             params
           )

    refute CommentedIssues.reacts_to?(
             comment(project_id: 2) |> Upkeep.Change.inserted(),
             params
           )
  end

  test "preloads add broad dependencies for associated schemas" do
    params = %{project_id: 1}

    assert PreloadedProjectIssues.__upkeep_interest_keys__(params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Column},
               {:upkeep_change, :updated, Column},
               {:upkeep_change, :deleted, Column},
               {:upkeep_change, :inserted, Comment},
               {:upkeep_change, :updated, Comment},
               {:upkeep_change, :deleted, Comment},
               {:upkeep_change, :inserted, IssueTag},
               {:upkeep_change, :updated, IssueTag},
               {:upkeep_change, :deleted, IssueTag},
               {:upkeep_change, :inserted, Issue, [project_id: 1]},
               {:upkeep_change, :updated, Issue, [project_id: 1]},
               {:upkeep_change, :deleted, Issue, [project_id: 1]},
               {:upkeep_change, :inserted, Tag},
               {:upkeep_change, :updated, Tag},
               {:upkeep_change, :deleted, Tag}
             ]
             |> sort_terms()

    assert PreloadedProjectIssues.reacts_to?(
             comment(project_id: 99) |> Upkeep.Change.inserted(),
             params
           )
  end

  test "coverage classifies preload deps as broad but known" do
    Repo.insert!(%Column{id: 1, project_id: 1, name: "Backlog", position: 1})
    Repo.insert!(issue(id: 1, project_id: 1, column_id: 1, title: "With preloads"))

    coverage = Upkeep.Test.assert_source_reactive!(PreloadedProjectIssues, %{project_id: 1})

    assert coverage.unknown == []
    assert %{schema: Issue, fields: [:project_id]} in coverage.precise
    assert %{schema: Column, reason: :no_precise_filters} in coverage.broad
    assert %{schema: Comment, reason: :no_precise_filters} in coverage.broad
    assert %{schema: Tag, reason: :no_precise_filters} in coverage.broad
    assert %{schema: IssueTag, reason: :no_precise_filters} in coverage.broad
  end

  test "coverage reports query preloads as unknown" do
    Repo.insert!(issue(id: 1, project_id: 1, title: "With query preload"))

    coverage = Upkeep.Source.coverage(QueryPreloadedProjectIssues, %{project_id: 1})

    assert %{reason: :unsupported_preload_query} in coverage.unknown
    assert %{schema: Comment, reason: :no_precise_filters} in coverage.broad

    assert_raise ExUnit.AssertionError, ~r/unknown entries/, fn ->
      Upkeep.Test.assert_source_reactive!(QueryPreloadedProjectIssues, %{project_id: 1})
    end
  end

  test "preloaded query sources refresh after associated records change" do
    Repo.insert!(%Column{id: 1, project_id: 1, name: "Backlog", position: 1})
    Repo.insert!(issue(id: 1, project_id: 1, column_id: 1, title: "With comments"), upkeep: false)

    socket =
      new_socket()
      |> Live.watch(:issues, PreloadedProjectIssues, project_id: 1)

    assert [%Issue{comments: []}] = socket.assigns.issues

    {:ok, %Comment{}} =
      Upkeep.mutate(fn ->
        Repo.insert!(comment(id: 1, project_id: 1, issue_id: 1, body: "New comment"))
      end)

    :ok = Upkeep.Coordinator.Graph.drain()

    source_id = {PreloadedProjectIssues, %{project_id: 1}}

    assert_receive {:dag_values,
                    [{^source_id, [%Issue{comments: [%Comment{body: "New comment"}]}]}]},
                   1_000
  end

  test "fragments intentionally fall back to broad schema invalidation" do
    params = %{project_id: 1, term: "issue"}

    assert FragmentIssues.__upkeep_interest_keys__(params) == [
             {:upkeep_change, :inserted, Issue},
             {:upkeep_change, :updated, Issue},
             {:upkeep_change, :deleted, Issue}
           ]

    assert FragmentIssues.reacts_to?(
             issue(project_id: 2, title: "Other") |> Upkeep.Change.updated(),
             params
           )
  end

  test "schemaless query sources infer table-keyed interest from field filters" do
    params = %{project_id: 1, user_id: 9}

    assert SchemalessProjectIssues.__upkeep_interest_keys__(params) == [
             {:upkeep_change, :inserted, "upkeep_source_ecto_test_issues",
              [assignee_id: 9, project_id: 1, status: "open"]},
             {:upkeep_change, :updated, "upkeep_source_ecto_test_issues",
              [assignee_id: 9, project_id: 1, status: "open"]},
             {:upkeep_change, :deleted, "upkeep_source_ecto_test_issues",
              [assignee_id: 9, project_id: 1, status: "open"]}
           ]

    change =
      Upkeep.Change.changed(
        :inserted,
        %{project_id: 1, assignee_id: 9, status: "open"},
        schema: "upkeep_source_ecto_test_issues",
        record: %{project_id: 1, assignee_id: 9, status: "open"}
      )

    refute SchemalessProjectIssues.reacts_to?(
             %{change | record: %{project_id: 1, assignee_id: 10, status: "open"}},
             params
           )

    assert SchemalessProjectIssues.reacts_to?(change, params)
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

  defp column(attrs) do
    struct!(Column, Keyword.merge([id: 1, project_id: 1, name: "Backlog", position: 1], attrs))
  end

  defp comment(attrs) do
    struct!(Comment, Keyword.merge([id: 1, project_id: 1, issue_id: 1, body: "Comment"], attrs))
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

  defp sort_terms(terms), do: Enum.sort_by(terms, &inspect/1)
end
