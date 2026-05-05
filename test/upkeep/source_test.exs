defmodule Upkeep.SourceTest do
  use ExUnit.Case, async: false

  alias Upkeep.Live

  defmodule Issue do
    defstruct [:id, :project_id, :assignee_id, :column_id]
  end

  defmodule Column do
    defstruct [:id, :project_id]
  end

  defmodule BoardColumns do
    use Upkeep.Source

    query(fn s ->
      [{_key, value}] = :ets.lookup(Upkeep.SourceTest, {:board_columns, s.project_id})
      value
    end)

    invalidated_by(Issue, :updated, on: :project_id)
    invalidated_by(Issue, :inserted, on: :project_id)
    invalidated_by(:issue_moved, on: :project_id)
  end

  defmodule MyIssues do
    use Upkeep.Source

    query(fn s ->
      [{_key, value}] = :ets.lookup(Upkeep.SourceTest, {:my_issues, s.project_id, s.user_id})
      value
    end)

    invalidated_by(Issue, :updated, on: [:project_id, :assignee_id], as: [:project_id, :user_id])

    reacts_to(:issue_assigned, fn change, s ->
      change.record.project_id == s.project_id and
        (change.from.assignee_id == s.user_id or change.record.assignee_id == s.user_id)
    end)
  end

  defmodule ColumnIssues do
    use Upkeep.Source

    query(fn _s -> [] end)

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

  test "source invalidators produce field-indexed interest keys" do
    assert BoardColumns.__upkeep_interest_keys__(%{project_id: 123}) == [
             {:upkeep_change, :issue_moved, :_, [project_id: 123]},
             {:upkeep_change, :inserted, Issue, [project_id: 123]},
             {:upkeep_change, :updated, Issue, [project_id: 123]}
           ]

    assert MyIssues.__upkeep_interest_keys__(%{project_id: 123, user_id: 9}) == [
             {:upkeep_change, :updated, Issue, [assignee_id: 9, project_id: 123]},
             {:upkeep_change, :issue_assigned, :_}
           ]
  end

  test "declarative invalidators and custom reactors decide whether a source reacts" do
    assert BoardColumns.reacts_to?(updated_issue(project_id: 123), %{project_id: 123})
    assert BoardColumns.reacts_to?(inserted_issue(project_id: 123), %{project_id: 123})
    assert BoardColumns.reacts_to?(issue_moved(project_id: 123), %{project_id: 123})
    refute BoardColumns.reacts_to?(updated_issue(project_id: 456), %{project_id: 123})

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
    assert NoRetryLoad.__upkeep_retry__() == false
    assert Upkeep.Source.retry_config(NoRetryLoad) == false

    assert CustomRetryLoad.__upkeep_retry__() == [
             max_attempts: 1,
             base_delay_ms: 0,
             max_delay_ms: 0
           ]

    assert Upkeep.Source.retry_config(BoardColumns) == :default
  end

  test "watch joins source interest and notify dispatches through the coordinator", %{
    table: table
  } do
    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

    socket = Live.watch(socket, :columns, BoardColumns, project_id: 123)
    assert socket.assigns.columns == [:todo]

    :ets.insert(table, {{:board_columns, 123}, [:todo, :doing]})

    change = updated_issue(project_id: 123)

    assert :ok = Upkeep.notify(change)
    :ok = Upkeep.Coordinator.Graph.drain()

    source_id = {BoardColumns, %{project_id: 123}}
    assert_receive {:dag_values, [{^source_id, [:todo, :doing]}]}

    refreshed = Live.apply_dag_value(socket, source_id, [:todo, :doing])
    assert refreshed.assigns.columns == [:todo, :doing]
  end

  test "coordinator does not dispatch to unrelated field-indexed watchers" do
    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

    Live.watch(socket, :columns, BoardColumns, project_id: 123)

    change = updated_issue(project_id: 456)

    assert :ok = Upkeep.notify(change)
    :ok = Upkeep.Coordinator.Graph.drain()

    refute_received {:dag_values, [{_, _}]}
  end

  test "coordinator sends one event when a process watches overlapping source keys" do
    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    project_id = System.unique_integer([:positive])
    user_id = 9

    :ets.insert(__MODULE__, {{:board_columns, project_id}, [:todo]})
    :ets.insert(__MODULE__, {{:my_issues, project_id, user_id}, [:mine]})

    socket = Live.watch(socket, :columns, BoardColumns, project_id: project_id)
    Live.watch(socket, :my_issues, MyIssues, project_id: project_id, user_id: user_id)

    assert socket.assigns.columns == [:todo]

    change = updated_issue(project_id: project_id, assignee_id: user_id)

    assert :ok = Upkeep.notify(change)

    assert_dag_values([
      {{BoardColumns, %{project_id: project_id}}, [:todo]},
      {{MyIssues, %{project_id: project_id, user_id: user_id}}, [:mine]}
    ])

    refute_received {:dag_values, _}
  end

  defp assert_dag_values(expected, received \\ []) do
    if Enum.all?(expected, &(&1 in received)) do
      received
    else
      receive do
        {:dag_values, batch} -> assert_dag_values(expected, received ++ batch)
      after
        1_000 ->
          flunk("expected DAG values #{inspect(expected)}, got #{inspect(received)}")
      end
    end
  end

  defp inserted_issue(attrs) do
    attrs
    |> issue()
    |> Upkeep.Change.inserted()
  end

  defp updated_issue(attrs) do
    {from, attrs} = Keyword.pop(attrs, :from)

    attrs
    |> issue()
    |> Upkeep.Change.updated(from: from)
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
