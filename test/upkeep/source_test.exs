defmodule Upkeep.SourceTest do
  use ExUnit.Case, async: false

  alias Upkeep.InvalidationSurface
  alias Upkeep.Live
  alias Upkeep.TestSupport.{DagMessages, LiveSocket}

  defmodule Issue do
    defstruct [:id, :project_id, :assignee_id, :column_id]
  end

  defmodule Column do
    defstruct [:id, :project_id]
  end

  defmodule BoardColumns do
    use Upkeep.Source

    def load(s) do
      [{_key, value}] = :ets.lookup(Upkeep.SourceTest, {:board_columns, s.project_id})
      value
    end

    invalidated_by(Issue, :updated, on: :project_id)
    invalidated_by(Issue, :inserted, on: :project_id)
    invalidated_by(:issue_moved, on: :project_id)
  end

  defmodule MyIssues do
    use Upkeep.Source

    def load(s) do
      [{_key, value}] = :ets.lookup(Upkeep.SourceTest, {:my_issues, s.project_id, s.user_id})
      value
    end

    invalidated_by(Issue, :updated, on: [:project_id, :assignee_id], as: [:project_id, :user_id])

    reacts_to(:issue_assigned, fn change, s ->
      change.record.project_id == s.project_id and
        (change.from.assignee_id == s.user_id or change.record.assignee_id == s.user_id)
    end)
  end

  defmodule ColumnIssues do
    use Upkeep.Source

    def load(_params), do: []

    reacts_to(Issue, :updated, fn change, s ->
      Upkeep.Change.changed?(change, :column_id) and
        (change.from.column_id == s.column_id or change.record.column_id == s.column_id)
    end)
  end

  defmodule HiddenLoad do
    use Upkeep.Source

    def load(_params), do: []
  end

  defmodule NoRetryLoad do
    use Upkeep.Source, retry: false

    def load(_params), do: []
  end

  defmodule CustomRetryLoad do
    use Upkeep.Source, retry: [max_attempts: 1, base_delay_ms: 0, max_delay_ms: 0]

    def load(_params), do: []
  end

  setup do
    Upkeep.Test.reset_graph()

    if :ets.info(__MODULE__) != :undefined do
      :ets.delete(__MODULE__)
    end

    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:board_columns, 123}, [:todo]})
    :ets.insert(table, {{:my_issues, 123, 9}, [:mine]})

    on_exit(fn ->
      if :ets.info(__MODULE__) != :undefined do
        :ets.delete(__MODULE__)
      end
    end)

    %{table: table}
  end

  test "source invalidators produce field-indexed invalidation surfaces" do
    assert surface_keys(BoardColumns, %{project_id: 123}) == [
             {:upkeep_change, :issue_moved, :_, [project_id: 123]},
             {:upkeep_change, :inserted, Issue, [project_id: 123]},
             {:upkeep_change, :updated, Issue, [project_id: 123]}
           ]

    assert surface_keys(MyIssues, %{project_id: 123, user_id: 9}) == [
             {:upkeep_change, :updated, Issue, [assignee_id: 9, project_id: 123]},
             {:upkeep_change, :issue_assigned, :_}
           ]
  end

  test "source instance captures identity and static source facts" do
    instance = Upkeep.Source.instance(BoardColumns, project_id: 123)

    assert instance.source == BoardColumns
    assert instance.params == %{project_id: 123}
    assert instance.id == {BoardColumns, %{project_id: 123}}
    assert instance.repo == Application.get_env(:upkeep, :repo)
    assert instance.retry == :default
    assert instance.sharing_partition == %{project_id: 123}

    assert InvalidationSurface.keys(instance.surface) == [
             {:upkeep_change, :issue_moved, :_, [project_id: 123]},
             {:upkeep_change, :inserted, Issue, [project_id: 123]},
             {:upkeep_change, :updated, Issue, [project_id: 123]}
           ]
  end

  test "source load result captures value, observed deps, surface, and coverage" do
    result = Upkeep.Source.Loader.load_result(BoardColumns, %{project_id: 123})

    assert result.instance == Upkeep.Source.instance(BoardColumns, %{project_id: 123})
    assert result.value == [:todo]
    assert result.tracked_deps == []
    assert result.surface == result.instance.surface
    assert result.coverage.source == BoardColumns
    assert result.coverage.params == %{project_id: 123}
  end

  test "change event keys stay bounded by action and schema" do
    change =
      updated_issue(
        project_id: 123,
        assignee_id: 10,
        column_id: 2,
        from: %Issue{id: 1, project_id: 123, assignee_id: 9, column_id: 1}
      )

    assert InvalidationSurface.event_keys(change) == [
             {:upkeep_change, :updated, Issue},
             {:upkeep_change, :updated, :_}
           ]
  end

  test "invalidation surfaces index coarsely and match through exact source data" do
    params = %{project_id: 123}
    surface = BoardColumns.__upkeep_surface__(params)

    assert InvalidationSurface.index_keys(surface) == [
             {:upkeep_change, :issue_moved, :_},
             {:upkeep_change, :inserted, Issue},
             {:upkeep_change, :updated, Issue}
           ]

    assert InvalidationSurface.matches?(surface, inserted_issue(project_id: 123))
    refute InvalidationSurface.matches?(surface, inserted_issue(project_id: 456))
  end

  test "declarative invalidators and custom reactors decide whether a source reacts" do
    assert BoardColumns.reacts_to?(updated_issue(project_id: 123), %{project_id: 123})
    assert BoardColumns.reacts_to?(inserted_issue(project_id: 123), %{project_id: 123})
    assert BoardColumns.reacts_to?(issue_moved(project_id: 123), %{project_id: 123})

    refute BoardColumns.reacts_to?(
             updated_issue(
               project_id: 456,
               from: %Issue{id: 1, project_id: 456, assignee_id: 9, column_id: 1}
             ),
             %{project_id: 123}
           )

    assert MyIssues.reacts_to?(
             updated_issue(project_id: 123, assignee_id: 9),
             %{project_id: 123, user_id: 9}
           )

    assert MyIssues.reacts_to?(
             issue_assigned(project_id: 123, from_assignee_id: 9, assignee_id: 10),
             %{project_id: 123, user_id: 9}
           )

    refute MyIssues.reacts_to?(
             issue_assigned(project_id: 123, from_assignee_id: 10, assignee_id: 11),
             %{project_id: 123, user_id: 9}
           )
  end

  test "updated changes match declarative keys from old and new records" do
    change =
      updated_issue(
        project_id: 123,
        assignee_id: 10,
        from: %Issue{id: 1, project_id: 123, assignee_id: 9}
      )

    assert MyIssues.reacts_to?(change, %{project_id: 123, user_id: 9})
    assert MyIssues.reacts_to?(change, %{project_id: 123, user_id: 10})
    refute MyIssues.reacts_to?(change, %{project_id: 123, user_id: 11})
  end

  test "updates without old state match declarative keys broadly" do
    change = updated_issue(project_id: 456)

    assert Upkeep.Change.broad_update?(change)
    assert BoardColumns.reacts_to?(change, %{project_id: 123})

    assert InvalidationSurface.event_keys(change) == [
             {:upkeep_change, :updated, Issue},
             {:upkeep_change, :updated, :_}
           ]
  end

  test "partial updates match changed declarative fields conservatively" do
    change =
      updated_issue(project_id: 123, assignee_id: 10, changed_fields: [:assignee_id])

    refute Upkeep.Change.broad_update?(change)
    assert Upkeep.Change.partial_update?(change)
    assert Upkeep.Change.field_change(change, :assignee_id) == :changed
    assert Upkeep.Change.field_change(change, :project_id) == :unchanged

    assert MyIssues.reacts_to?(change, %{project_id: 123, user_id: 9})
    assert MyIssues.reacts_to?(change, %{project_id: 123, user_id: 10})
    refute MyIssues.reacts_to?(change, %{project_id: 456, user_id: 9})

    assert BoardColumns.reacts_to?(change, %{project_id: 123})
    refute BoardColumns.reacts_to?(change, %{project_id: 456})
  end

  test "custom reactors can inspect old and new record state" do
    change =
      updated_issue(
        column_id: 2,
        from: %Issue{id: 1, project_id: 123, assignee_id: 9, column_id: 1}
      )

    assert ColumnIssues.reacts_to?(change, %{column_id: 1})
    assert ColumnIssues.reacts_to?(change, %{column_id: 2})
    refute ColumnIssues.reacts_to?(change, %{column_id: 3})
  end

  test "coverage reports unknown sources that do not declare or track reads" do
    coverage = Upkeep.Source.coverage(HiddenLoad, %{})

    assert coverage.unknown == [%{reason: :no_invalidation_surface}]
    assert Upkeep.Source.Coverage.summary(coverage) =~ "non-reactive"

    assert [
             %{
               reason: :no_invalidation_surface,
               severity: :error,
               action: action
             }
           ] = Upkeep.Source.Coverage.diagnostics(coverage)

    assert action =~ "Upkeep.read/1"

    error =
      assert_raise ExUnit.AssertionError, fn ->
        Upkeep.Test.assert_source_reactive!(HiddenLoad, %{})
      end

    assert error.message =~ "known Upkeep invalidation surface"
    assert error.message =~ "no invalidation surface"
    assert error.message =~ "Add invalidated_by/reacts_to declarations"
  end

  test "coverage explanations include captured source location when available" do
    coverage = Upkeep.Source.coverage(HiddenLoad, %{})

    location = %{
      file_label: "test/live_view.ex",
      line: 42,
      snippet: "> 42  socket |> watch(:hidden, HiddenLoad, %{})"
    }

    explanation = Upkeep.Source.Coverage.explain(coverage, source_location: location)

    assert explanation =~ "Declared at test/live_view.ex:42"
    assert explanation =~ "watch(:hidden, HiddenLoad"

    assert [%{location_label: "test/live_view.ex:42", source_excerpt: source_excerpt}] =
             Upkeep.Source.Coverage.diagnostics(coverage, source_location: location)

    assert source_excerpt =~ "watch(:hidden, HiddenLoad"
  end

  test "sources expose retry configuration" do
    assert Upkeep.Source.instance(NoRetryLoad, %{}).retry == false

    assert Upkeep.Source.instance(CustomRetryLoad, %{}).retry == [
             max_attempts: 1,
             base_delay_ms: 0,
             max_delay_ms: 0
           ]

    assert Upkeep.Source.instance(BoardColumns, %{project_id: 123}).retry == :default
  end

  test "watch joins source interest and notify dispatches through the coordinator", %{
    table: table
  } do
    socket = LiveSocket.socket()

    socket = Live.watch(socket, :columns, BoardColumns, project_id: 123)
    assert socket.assigns.columns == [:todo]

    :ets.insert(table, {{:board_columns, 123}, [:todo, :doing]})

    change = updated_issue(project_id: 123)

    assert :ok = Upkeep.notify(change)
    :ok = Upkeep.Test.drain()

    assert_board_columns_refresh(123, [:todo, :doing])

    refreshed = Live.apply_dag_value(socket, board_columns_source_id(123), [:todo, :doing])
    assert refreshed.assigns.columns == [:todo, :doing]
  end

  test "coordinator does not dispatch to unrelated field-indexed watchers" do
    socket = LiveSocket.socket()

    Live.watch(socket, :columns, BoardColumns, project_id: 123)

    change =
      updated_issue(
        project_id: 456,
        from: %Issue{id: 1, project_id: 456, assignee_id: 9, column_id: 1}
      )

    assert :ok = Upkeep.notify(change)
    :ok = Upkeep.Test.drain()

    refute_source_refresh()
  end

  test "coordinator dispatches field-indexed watchers broadly when update old state is missing" do
    socket = LiveSocket.socket()

    Live.watch(socket, :columns, BoardColumns, project_id: 123)

    change = updated_issue(project_id: 456)

    assert :ok = Upkeep.notify(change)
    :ok = Upkeep.Test.drain()

    assert_board_columns_refresh(123, [:todo])
  end

  test "coordinator sends one event when a process watches overlapping source keys" do
    socket = LiveSocket.socket()
    project_id = System.unique_integer([:positive])
    user_id = 9

    :ets.insert(__MODULE__, {{:board_columns, project_id}, [:todo]})
    :ets.insert(__MODULE__, {{:my_issues, project_id, user_id}, [:mine]})

    socket = Live.watch(socket, :columns, BoardColumns, project_id: project_id)
    Live.watch(socket, :my_issues, MyIssues, project_id: project_id, user_id: user_id)

    assert socket.assigns.columns == [:todo]

    change = updated_issue(project_id: project_id, assignee_id: user_id)

    assert :ok = Upkeep.notify(change)

    assert_board_columns_refresh(project_id, [:todo])
    assert_my_issues_refresh(project_id, user_id, [:mine])
    refute_source_refresh()
  end

  defp assert_board_columns_refresh(project_id, columns) do
    assert DagMessages.receive_value(board_columns_source_id(project_id)) == columns
  end

  defp assert_my_issues_refresh(project_id, user_id, issues) do
    assert DagMessages.receive_value({MyIssues, %{project_id: project_id, user_id: user_id}}) ==
             issues
  end

  defp refute_source_refresh do
    DagMessages.refute_any()
  end

  defp board_columns_source_id(project_id), do: {BoardColumns, %{project_id: project_id}}

  defp surface_keys(source, params) do
    source
    |> apply(:__upkeep_surface__, [params])
    |> InvalidationSurface.keys()
  end

  defp inserted_issue(attrs) do
    attrs
    |> issue()
    |> Upkeep.Change.inserted()
  end

  defp updated_issue(attrs) do
    {from, attrs} = Keyword.pop(attrs, :from)
    {changed_fields, attrs} = Keyword.pop(attrs, :changed_fields)

    attrs
    |> issue()
    |> Upkeep.Change.updated(from: from, changed_fields: changed_fields)
  end

  defp issue_moved(attrs) do
    attrs
    |> issue()
    |> then(&Upkeep.Change.changed(:issue_moved, &1))
  end

  defp issue_assigned(attrs) do
    {from_assignee_id, attrs} = Keyword.pop(attrs, :from_assignee_id)
    issue = issue(attrs)
    from = %{issue | assignee_id: from_assignee_id}

    Upkeep.Change.changed(:issue_assigned, issue, from: from)
  end

  defp issue(attrs) do
    struct!(
      Issue,
      Keyword.merge([id: 1, project_id: 123, assignee_id: 9, column_id: 1], attrs)
    )
  end
end
