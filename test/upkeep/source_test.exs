defmodule Upkeep.SourceTest do
  use ExUnit.Case, async: false

  alias Upkeep.InvalidationSurface
  alias Upkeep.Live
  alias Upkeep.Source.Coverage
  alias Upkeep.Source.Loader
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

  defmodule SearchResults do
    use Upkeep.Source

    invalidated_by(:search_index_rebuilt, on: :project_id)

    def load(s) do
      [{_key, value}] = :ets.lookup(Upkeep.SourceTest, {:search_results, s.project_id})
      value
    end
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
    :ets.insert(table, {{:search_results, 123}, [:indexed]})

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
    result = Loader.load_result(BoardColumns, %{project_id: 123})

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

  test "bare named invalidators cover and match map-payload semantic changes" do
    params = %{project_id: 123}
    coverage = Upkeep.Source.coverage(SearchResults, params)

    assert coverage.unknown == []

    assert coverage.explicit == [
             {:upkeep_change, :search_index_rebuilt, :_, [project_id: 123]}
           ]

    assert SearchResults.reacts_to?(
             Upkeep.Change.changed(:search_index_rebuilt, %{project_id: 123}),
             params
           )

    refute SearchResults.reacts_to?(
             Upkeep.Change.changed(:search_index_rebuilt, %{project_id: 456}),
             params
           )
  end

  test "unloaded source modules are loaded before surface introspection" do
    source =
      unloaded_source_module!("ColdSearchResults", """
      use Upkeep.Source

      invalidated_by(:cold_search_rebuilt, on: :project_id)

      def load(%{project_id: project_id}) do
        {:cold, project_id}
      end
      """)

    params = %{project_id: 123}
    assert :code.is_loaded(source) == false

    result = Loader.load_result(source, params)

    assert result.value == {:cold, 123}
    assert result.coverage.unknown == []

    assert result.coverage.explicit == [
             {:upkeep_change, :cold_search_rebuilt, :_, [project_id: 123]}
           ]

    assert InvalidationSurface.keys(result.surface) == [
             {:upkeep_change, :cold_search_rebuilt, :_, [project_id: 123]}
           ]

    assert source.reacts_to?(
             Upkeep.Change.changed(:cold_search_rebuilt, %{project_id: 123}),
             params
           )
  end

  test "coverage reports unknown sources that do not declare or track reads" do
    coverage = Upkeep.Source.coverage(HiddenLoad, %{})

    assert coverage.unknown == [%{reason: :no_invalidation_surface}]
    assert Coverage.summary(coverage) =~ "non-reactive"

    assert [
             %{
               reason: :no_invalidation_surface,
               severity: :error,
               action: action
             }
           ] = Coverage.diagnostics(coverage)

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

    explanation = Coverage.explain(coverage, source_location: location)

    assert explanation =~ "Declared at test/live_view.ex:42"
    assert explanation =~ "watch(:hidden, HiddenLoad"

    assert [%{location_label: "test/live_view.ex:42", source_excerpt: source_excerpt}] =
             Coverage.diagnostics(coverage, source_location: location)

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

    Upkeep.Test.sync(fn ->
      assert :ok = Upkeep.notify(change)
    end)

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

    Upkeep.Test.sync(fn ->
      assert :ok = Upkeep.notify(change)
    end)

    refute_source_refresh()
  end

  test "coordinator dispatches field-indexed watchers broadly when update old state is missing" do
    socket = LiveSocket.socket()

    Live.watch(socket, :columns, BoardColumns, project_id: 123)

    change = updated_issue(project_id: 456)

    Upkeep.Test.sync(fn ->
      assert :ok = Upkeep.notify(change)
    end)

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

  test "coordinator refreshes bare named invalidators from map-payload semantic changes", %{
    table: table
  } do
    socket = LiveSocket.socket()

    socket = Live.watch(socket, :results, SearchResults, project_id: 123)
    assert socket.assigns.results == [:indexed]

    :ets.insert(table, {{:search_results, 123}, [:reindexed]})

    assert :ok = Upkeep.changed(:search_index_rebuilt, %{project_id: 123})
    :ok = Upkeep.Test.await_idle()

    assert DagMessages.receive_value({SearchResults, %{project_id: 123}}) == [:reindexed]
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
    source.__upkeep_surface__(params)
    |> InvalidationSurface.keys()
  end

  defp unloaded_source_module!(name, body) do
    module = Module.concat(__MODULE__, "#{name}#{System.unique_integer([:positive])}")
    dir = Path.join(System.tmp_dir!(), "upkeep_source_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    [{^module, beam}] = Code.compile_string("defmodule #{inspect(module)} do\n#{body}\nend")
    beam_path = Path.join(dir, "#{module}.beam")
    File.write!(beam_path, beam)

    :code.purge(module)
    :code.delete(module)
    :code.purge(module)
    true = :code.add_patha(String.to_charlist(dir))

    on_exit(fn ->
      :code.purge(module)
      :code.delete(module)
      :code.purge(module)
      :code.del_path(String.to_charlist(dir))
      File.rm_rf(dir)
    end)

    module
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
