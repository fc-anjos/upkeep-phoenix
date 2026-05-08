defmodule Upkeep.SourceEctoTest do
  use Upkeep.TestSupport.DataCase, async: false

  alias Upkeep.InvalidationSurface
  alias Upkeep.Live
  alias Upkeep.TestSupport.{DagProbe, LiveSocket}

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

  defmodule IssueWithStringTags do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_source_ecto_test_issues" do
      field :project_id, :integer
      field :title, :string

      many_to_many :tags, Upkeep.SourceEctoTest.Tag,
        join_through: "upkeep_source_ecto_test_issue_tags",
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

      belongs_to :issue, Upkeep.SourceEctoTest.Issue, define_field: false
    end
  end

  defmodule ProjectIssues do
    use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

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
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, user_id: user_id}) do
      from i in Issue,
        where: i.project_id == ^project_id or i.assignee_id == ^user_id
    end
  end

  defmodule UnsupportedOrProjectIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, term: term}) do
      from i in Issue,
        where: i.project_id == ^project_id or fragment("lower(?)", i.title) == ^term
    end
  end

  defmodule TernaryOrProjectIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, user_id: user_id, status: status}) do
      from i in Issue,
        where: i.project_id == ^project_id or i.assignee_id == ^user_id or i.status == ^status
    end
  end

  defmodule FieldComparisonIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(_params) do
      from i in Issue,
        where: i.project_id == i.assignee_id
    end
  end

  defmodule JoinedIssueCards do
    use Upkeep.Ecto.Source

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
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id}) do
      from i in Issue,
        join: c in assoc(i, :column),
        where: i.project_id == ^project_id and c.project_id == ^project_id,
        select: {i.id, c.name}
    end
  end

  defmodule NestedJoinedPreloadedComments do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Comment

    def query(%{project_id: project_id}) do
      from c in Comment,
        join: i in assoc(c, :issue),
        join: column in assoc(i, :column),
        where: c.project_id == ^project_id,
        preload: [issue: {i, column: column}]
    end
  end

  defmodule NamedJoinedPreloadedIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id}) do
      from i in Issue,
        as: :issue,
        join: c in assoc(i, :comments),
        as: :comment,
        where: i.project_id == ^project_id,
        preload: [comments: c]
    end
  end

  defmodule DynamicJoinedPreloadedIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id}) do
      preloads = [comments: dynamic([comment: c], c)]

      from i in Issue,
        join: c in assoc(i, :comments),
        as: :comment,
        where: i.project_id == ^project_id,
        preload: ^preloads
    end
  end

  defmodule MixedJoinedAndQueryPreloadedIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id}) do
      from i in Issue,
        join: c in assoc(i, :comments),
        where: i.project_id == ^project_id,
        preload: [comments: c, column: []]
    end
  end

  defmodule JoinedColumnProjection do
    use Upkeep.Ecto.Source

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
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id, statuses: statuses}) do
      filters = dynamic([i], i.project_id == ^project_id and i.status in ^statuses)

      from i in Issue,
        where: ^filters
    end
  end

  defmodule CommentedIssues do
    use Upkeep.Ecto.Source

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

  defmodule JoinedFragmentIssues do
    use Upkeep.Ecto.Source

    import Ecto.Query

    alias Upkeep.SourceEctoTest.{Column, Issue}

    def query(%{project_id: project_id, term: term}) do
      from i in Issue,
        join: c in Column,
        on: c.id == i.column_id,
        where: i.project_id == ^project_id and fragment("lower(?)", c.name) == ^term
    end
  end

  defmodule PreloadedProjectIssues do
    use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def query(%{project_id: project_id}) do
      from i in Issue,
        where: i.project_id == ^project_id,
        order_by: [asc: i.position],
        preload: [:column, :comments, :tags]
    end
  end

  defmodule StringManyToManyPreloadedIssues do
    use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.IssueWithStringTags

    def query(%{project_id: project_id}) do
      from i in IssueWithStringTags,
        where: i.project_id == ^project_id,
        preload: [:tags]
    end
  end

  defmodule QueryPreloadedProjectIssues do
    use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

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

  defmodule NestedQueryPreloadedComments do
    use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.{Comment, Issue}

    def query(%{project_id: project_id}) do
      issue_query =
        from i in Issue,
          where: i.project_id == ^project_id

      from c in Comment,
        where: c.body == "visible",
        preload: [issue: ^issue_query]
    end
  end

  defmodule SchemalessProjectIssues do
    use Upkeep.Ecto.Source

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
    use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

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

  test "plain query/1 sources load through their configured repo" do
    Repo.insert!(issue(id: 1, project_id: 1, assignee_id: 9, title: "Mine", position: 2))
    Repo.insert!(issue(id: 2, project_id: 1, assignee_id: 10, title: "Other", position: 1))
    Repo.insert!(issue(id: 3, project_id: 1, assignee_id: 9, title: "First", position: 1))

    result = Upkeep.Source.Loader.load_result(ProjectIssues, %{project_id: 1, user_id: 9})

    assert Enum.map(result.value, & &1.title) == ["First", "Mine"]
  end

  defmodule RepeatingReadSource do
    use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

    import Ecto.Query

    alias Upkeep.SourceEctoTest.Issue

    def load(%{project_id: project_id}) do
      q = from(i in Issue, where: i.project_id == ^project_id, order_by: [asc: i.position])
      first = Upkeep.read(q)
      second = Upkeep.read(q)
      {first, second}
    end
  end

  test "Upkeep.read memoizes identical queries within a single source.load" do
    Repo.insert!(%Column{id: 1, project_id: 1, name: "Backlog", position: 1})
    Repo.insert!(issue(id: 1, project_id: 1, column_id: 1, title: "Open", position: 1))

    counter = :counters.new(1, [])
    handler_id = {__MODULE__, :memo_test}

    :telemetry.attach(
      handler_id,
      [:upkeep, :repo, :query],
      fn _event, _measurements, _meta, _config ->
        :counters.add(counter, 1, 1)
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    result = Upkeep.Source.Loader.load_result(RepeatingReadSource, %{project_id: 1})
    {first, second} = result.value

    assert first == second
    # The two Upkeep.read calls share one underlying repo.all
    assert :counters.get(counter, 1) <= 1,
           "expected at most one repo query, got #{:counters.get(counter, 1)}"
  end

  test "custom load/1 tracks each Upkeep.read query as the reactive surface" do
    Repo.insert!(%Column{id: 1, project_id: 1, name: "Backlog", position: 1})
    Repo.insert!(issue(id: 1, project_id: 1, column_id: 1, title: "Open", position: 1))
    Repo.insert!(issue(id: 2, project_id: 1, column_id: 1, status: "archived", title: "Archived"))

    result = Upkeep.Source.Loader.load_result(BoardViewModel, %{project_id: 1})

    assert [%{column: "Backlog", issues: [%Issue{title: "Open"}]}] = result.value

    result_keys = InvalidationSurface.keys(result.surface)

    assert {:upkeep_change, :inserted, Column, [project_id: 1]} in result_keys

    assert {:upkeep_change, :updated, Issue, [project_id: 1, status: "open"]} in result_keys

    assert InvalidationSurface.matches?(
             Upkeep.Source.dependency_surface(result.tracked_deps),
             issue(project_id: 1, column_id: 1, status: "open") |> Upkeep.Change.inserted()
           )

    refute InvalidationSurface.matches?(
             Upkeep.Source.dependency_surface(result.tracked_deps),
             issue(project_id: 1, column_id: 1, status: "archived") |> Upkeep.Change.inserted()
           )
  end

  test "plain query/1 sources infer field-indexed surfaces from Ecto where clauses" do
    assert surface_keys(ProjectIssues, %{project_id: 1, user_id: 9}) == [
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
      LiveSocket.socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1, user_id: 9)

    assert Enum.map(socket.assigns.issues, & &1.title) == ["Before"]

    {:ok, %Issue{}} =
      Upkeep.mutate(fn ->
        Repo.insert!(issue(id: 2, project_id: 1, assignee_id: 9, title: "After", position: 2))
      end)

    source_id = {ProjectIssues, %{project_id: 1, user_id: 9}}

    issues = DagProbe.receive_value(source_id)
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

    assert ProjectIssues.reacts_to?(
             issue(project_id: 1, assignee_id: 10, status: "open") |> Upkeep.Change.updated(),
             params
           )

    refute ProjectIssues.reacts_to?(
             issue(project_id: 1, assignee_id: 10, status: "open")
             |> Upkeep.Change.updated(from: issue(project_id: 1, assignee_id: 10, status: "open")),
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

  test "plain query/1 sources use partial update fields without losing stable filters" do
    params = %{project_id: 1, user_id: 9}

    assignee_change =
      issue(project_id: 1, assignee_id: 10, status: "open")
      |> Upkeep.Change.updated(changed_fields: [:assignee_id])

    assert ProjectIssues.reacts_to?(assignee_change, params)
    assert ProjectIssues.reacts_to?(assignee_change, %{project_id: 1, user_id: 10})
    refute ProjectIssues.reacts_to?(assignee_change, %{project_id: 2, user_id: 9})

    status_change =
      issue(project_id: 1, assignee_id: 9, status: "archived")
      |> Upkeep.Change.updated(changed_fields: [:status])

    assert ProjectIssues.reacts_to?(status_change, params)

    unrelated_project =
      issue(project_id: 2, assignee_id: 9, status: "archived")
      |> Upkeep.Change.updated(changed_fields: [:status])

    refute ProjectIssues.reacts_to?(unrelated_project, params)
  end

  test "simple or query shapes infer precise alternative invalidation keys" do
    assert surface_keys(BroadProjectIssues, %{project_id: 1, user_id: 9})
           |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Issue, [assignee_id: 9]},
               {:upkeep_change, :updated, Issue, [assignee_id: 9]},
               {:upkeep_change, :deleted, Issue, [assignee_id: 9]},
               {:upkeep_change, :inserted, Issue, [project_id: 1]},
               {:upkeep_change, :updated, Issue, [project_id: 1]},
               {:upkeep_change, :deleted, Issue, [project_id: 1]}
             ]
             |> sort_terms()

    assert BroadProjectIssues.reacts_to?(
             issue(project_id: 2, assignee_id: 9, status: "open") |> Upkeep.Change.updated(),
             %{project_id: 1, user_id: 9}
           )

    assert BroadProjectIssues.reacts_to?(
             issue(project_id: 1, assignee_id: 10, status: "open") |> Upkeep.Change.updated(),
             %{project_id: 1, user_id: 9}
           )

    assert BroadProjectIssues.reacts_to?(
             issue(project_id: 2, assignee_id: 10, status: "open") |> Upkeep.Change.updated(),
             %{project_id: 1, user_id: 9}
           )

    refute BroadProjectIssues.reacts_to?(
             issue(project_id: 2, assignee_id: 10, status: "open")
             |> Upkeep.Change.updated(from: issue(project_id: 2, assignee_id: 10, status: "open")),
             %{project_id: 1, user_id: 9}
           )

    assert surface_keys(TernaryOrProjectIssues, %{
             project_id: 1,
             user_id: 9,
             status: "open"
           })
           |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Issue, [assignee_id: 9]},
               {:upkeep_change, :updated, Issue, [assignee_id: 9]},
               {:upkeep_change, :deleted, Issue, [assignee_id: 9]},
               {:upkeep_change, :inserted, Issue, [project_id: 1]},
               {:upkeep_change, :updated, Issue, [project_id: 1]},
               {:upkeep_change, :deleted, Issue, [project_id: 1]},
               {:upkeep_change, :inserted, Issue, [status: "open"]},
               {:upkeep_change, :updated, Issue, [status: "open"]},
               {:upkeep_change, :deleted, Issue, [status: "open"]}
             ]
             |> sort_terms()
  end

  test "unsupported or query branches fall back to broad schema invalidation with reasons" do
    params = %{project_id: 1, term: "issue"}

    assert surface_keys(UnsupportedOrProjectIssues, params) == [
             {:upkeep_change, :inserted, Issue},
             {:upkeep_change, :updated, Issue},
             {:upkeep_change, :deleted, Issue}
           ]

    coverage = Upkeep.Source.coverage(UnsupportedOrProjectIssues, params)

    assert coverage.unknown == []
    assert %{schema: Issue, reason: :fragment} in coverage.broad
    assert %{schema: Issue, reason: :unsupported_or} in coverage.broad

    diagnostics = Upkeep.Source.Coverage.diagnostics(coverage)

    assert Enum.any?(diagnostics, &(&1.reason == :fragment and &1.action =~ "regular Ecto"))
    assert Enum.any?(diagnostics, &(&1.reason == :unsupported_or and &1.action =~ "equality/in"))
  end

  test "field-to-field comparisons deopt with a value-expression reason" do
    coverage = Upkeep.Source.coverage(FieldComparisonIssues, %{})

    assert surface_keys(FieldComparisonIssues, %{}) == [
             {:upkeep_change, :inserted, Issue},
             {:upkeep_change, :updated, Issue},
             {:upkeep_change, :deleted, Issue}
           ]

    assert coverage.unknown == []
    assert %{schema: Issue, reason: :unsupported_value_expression} in coverage.broad

    assert [
             %{
               schema: Issue,
               reason: :unsupported_value_expression,
               label: "unsupported value expression"
             }
           ] =
             Upkeep.Source.Coverage.diagnostics(coverage)
  end

  test "joined queries infer dependencies for every schema with equality filters" do
    params = %{project_id: 1, statuses: ["open", "blocked"]}

    assert surface_keys(JoinedIssueCards, params) |> sort_terms() ==
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
             issue(project_id: 1, status: "closed")
             |> Upkeep.Change.updated(from: issue(project_id: 1, status: "closed")),
             params
           )

    assert JoinedIssueCards.reacts_to?(
             column(project_id: 1) |> Upkeep.Change.updated(),
             params
           )

    refute JoinedIssueCards.reacts_to?(
             column(project_id: 2) |> Upkeep.Change.updated(from: column(project_id: 2)),
             params
           )
  end

  test "assoc joins infer the related schema from Ecto association metadata" do
    params = %{project_id: 1}

    assert surface_keys(AssocJoinedIssueCards, params) |> sort_terms() ==
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

  test "joined preload edge shapes add broad associated dependencies" do
    params = %{project_id: 1}

    assert surface_keys(NestedJoinedPreloadedComments, params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Column},
               {:upkeep_change, :updated, Column},
               {:upkeep_change, :deleted, Column},
               {:upkeep_change, :inserted, Comment, [project_id: 1]},
               {:upkeep_change, :updated, Comment, [project_id: 1]},
               {:upkeep_change, :deleted, Comment, [project_id: 1]},
               {:upkeep_change, :inserted, Issue},
               {:upkeep_change, :updated, Issue},
               {:upkeep_change, :deleted, Issue}
             ]
             |> sort_terms()

    for source <- [
          NamedJoinedPreloadedIssues,
          DynamicJoinedPreloadedIssues,
          MixedJoinedAndQueryPreloadedIssues
        ] do
      coverage = Upkeep.Source.coverage(source, params)

      assert %{schema: Issue, fields: [:project_id]} in coverage.precise
      assert %{schema: Comment, reason: :preload} in coverage.broad
    end

    mixed_coverage = Upkeep.Source.coverage(MixedJoinedAndQueryPreloadedIssues, params)
    assert %{schema: Column, reason: :preload} in mixed_coverage.broad
  end

  test "joined projections without equality filters fall back only for that schema" do
    params = %{project_id: 1}

    assert surface_keys(JoinedColumnProjection, params) |> sort_terms() ==
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

    assert surface_keys(DynamicIssues, params) |> sort_terms() ==
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
             issue(project_id: 1, status: "closed")
             |> Upkeep.Change.updated(from: issue(project_id: 1, status: "closed")),
             params
           )
  end

  test "subqueries add dependencies for their inner schemas" do
    params = %{project_id: 1}

    assert surface_keys(CommentedIssues, params) |> sort_terms() ==
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

    assert surface_keys(PreloadedProjectIssues, params) |> sort_terms() ==
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
    assert %{schema: Column, reason: :preload} in coverage.broad
    assert %{schema: Comment, reason: :preload} in coverage.broad
    assert %{schema: Tag, reason: :preload} in coverage.broad
    assert %{schema: IssueTag, reason: :many_to_many_join} in coverage.broad
  end

  test "string many-to-many preloads add broad table dependencies" do
    params = %{project_id: 1}

    assert surface_keys(StringManyToManyPreloadedIssues, params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, "upkeep_source_ecto_test_issue_tags"},
               {:upkeep_change, :updated, "upkeep_source_ecto_test_issue_tags"},
               {:upkeep_change, :deleted, "upkeep_source_ecto_test_issue_tags"},
               {:upkeep_change, :inserted, IssueWithStringTags, [project_id: 1]},
               {:upkeep_change, :updated, IssueWithStringTags, [project_id: 1]},
               {:upkeep_change, :deleted, IssueWithStringTags, [project_id: 1]},
               {:upkeep_change, :inserted, Tag},
               {:upkeep_change, :updated, Tag},
               {:upkeep_change, :deleted, Tag}
             ]
             |> sort_terms()

    coverage = Upkeep.Source.coverage(StringManyToManyPreloadedIssues, params)

    assert %{schema: IssueWithStringTags, fields: [:project_id]} in coverage.precise
    assert %{schema: Tag, reason: :preload} in coverage.broad

    assert %{schema: "upkeep_source_ecto_test_issue_tags", reason: :many_to_many_join} in coverage.broad
  end

  test "coverage merges query preload deps" do
    Repo.insert!(issue(id: 1, project_id: 1, title: "With query preload"))

    coverage = Upkeep.Test.assert_source_reactive!(QueryPreloadedProjectIssues, %{project_id: 1})

    assert coverage.unknown == []
    assert %{schema: Issue, fields: [:project_id]} in coverage.precise
    assert %{schema: Comment, fields: [:body]} in coverage.precise
    refute %{schema: Comment, reason: :no_precise_filters} in coverage.broad
  end

  test "coverage recursively merges nested query preload deps" do
    coverage = Upkeep.Test.assert_source_reactive!(NestedQueryPreloadedComments, %{project_id: 1})

    assert coverage.unknown == []
    assert %{schema: Comment, fields: [:body]} in coverage.precise
    assert %{schema: Issue, fields: [:project_id]} in coverage.precise
  end

  test "preloaded query sources refresh after associated records change" do
    Repo.insert!(%Column{id: 1, project_id: 1, name: "Backlog", position: 1})
    Repo.insert!(issue(id: 1, project_id: 1, column_id: 1, title: "With comments"), upkeep: false)

    socket =
      LiveSocket.socket()
      |> Live.watch(:issues, PreloadedProjectIssues, project_id: 1)

    assert [%Issue{comments: []}] = socket.assigns.issues

    {:ok, %Comment{}} =
      Upkeep.mutate(fn ->
        Repo.insert!(comment(id: 1, project_id: 1, issue_id: 1, body: "New comment"))
      end)

    source_id = {PreloadedProjectIssues, %{project_id: 1}}

    assert [%Issue{comments: [%Comment{body: "New comment"}]}] =
             DagProbe.receive_value(source_id)
  end

  test "query preload sources refresh from analyzed preload query deps" do
    Repo.insert!(issue(id: 1, project_id: 1, title: "With visible comments"), upkeep: false)

    LiveSocket.socket()
    |> Live.watch(:issues, QueryPreloadedProjectIssues, project_id: 1)

    {:ok, %Comment{}} =
      Upkeep.mutate(fn ->
        Repo.insert!(comment(id: 1, project_id: 1, issue_id: 1, body: "visible"))
      end)

    source_id = {QueryPreloadedProjectIssues, %{project_id: 1}}

    assert [%Issue{comments: [%Comment{body: "visible"}]}] = DagProbe.receive_value(source_id)
  end

  test "fragment broad fallback is scoped to the schema referenced by the fragment" do
    params = %{project_id: 1, term: "backlog"}

    assert surface_keys(JoinedFragmentIssues, params) |> sort_terms() ==
             [
               {:upkeep_change, :inserted, Column},
               {:upkeep_change, :updated, Column},
               {:upkeep_change, :deleted, Column},
               {:upkeep_change, :inserted, Issue, [project_id: 1]},
               {:upkeep_change, :updated, Issue, [project_id: 1]},
               {:upkeep_change, :deleted, Issue, [project_id: 1]}
             ]
             |> sort_terms()

    coverage = Upkeep.Source.coverage(JoinedFragmentIssues, params)

    assert %{schema: Issue, fields: [:project_id]} in coverage.precise
    assert %{schema: Column, reason: :fragment} in coverage.broad
  end

  test "schemaless query sources infer table-keyed interest from field filters" do
    params = %{project_id: 1, user_id: 9}

    assert surface_keys(SchemalessProjectIssues, params) == [
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

  defp surface_keys(source, params) do
    source
    |> apply(:__upkeep_surface__, [params])
    |> InvalidationSurface.keys()
  end

  defp sort_terms(terms), do: Enum.sort_by(terms, &inspect/1)
end
