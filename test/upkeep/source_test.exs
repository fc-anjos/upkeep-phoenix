defmodule Upkeep.SourceTest do
  use ExUnit.Case, async: false

  alias Upkeep.Live

  defmodule IssueMoved do
    defstruct [:project_id, :assignee_id, :issue_id]
  end

  defmodule IssueAssigned do
    defstruct [:project_id, :old_assignee_id, :new_assignee_id]
  end

  defmodule BoardColumns do
    use Upkeep.Source

    query(fn s ->
      [{_key, value}] = :ets.lookup(Upkeep.SourceTest, {:board_columns, s.project_id})
      value
    end)

    invalidated_by(IssueMoved, on: :project_id)
  end

  defmodule MyIssues do
    use Upkeep.Source

    query(fn s ->
      [{_key, value}] = :ets.lookup(Upkeep.SourceTest, {:my_issues, s.project_id, s.user_id})
      value
    end)

    invalidated_by(IssueMoved, on: [:project_id, :assignee_id], as: [:project_id, :user_id])

    reacts_to(IssueAssigned, fn event, s ->
      event.project_id == s.project_id and
        (event.old_assignee_id == s.user_id or event.new_assignee_id == s.user_id)
    end)
  end

  setup do
    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:board_columns, 123}, [:todo]})
    :ets.insert(table, {{:my_issues, 123, 9}, [:mine]})

    %{table: table}
  end

  test "source invalidators produce field-indexed interest keys" do
    assert BoardColumns.__upkeep_interest_keys__(%{project_id: 123}) == [
             {:upkeep_event, IssueMoved, [project_id: 123]}
           ]

    assert MyIssues.__upkeep_interest_keys__(%{project_id: 123, user_id: 9}) == [
             {:upkeep_event, IssueMoved, [assignee_id: 9, project_id: 123]},
             {:upkeep_event, IssueAssigned}
           ]
  end

  test "declarative invalidators and custom reactors decide whether a source reacts" do
    assert BoardColumns.reacts_to?(%IssueMoved{project_id: 123}, %{project_id: 123})
    refute BoardColumns.reacts_to?(%IssueMoved{project_id: 456}, %{project_id: 123})

    assert MyIssues.reacts_to?(
             %IssueMoved{project_id: 123, assignee_id: 9},
             %{project_id: 123, user_id: 9}
           )

    assert MyIssues.reacts_to?(
             %IssueAssigned{project_id: 123, old_assignee_id: 9, new_assignee_id: 10},
             %{project_id: 123, user_id: 9}
           )

    refute MyIssues.reacts_to?(
             %IssueAssigned{project_id: 123, old_assignee_id: 10, new_assignee_id: 11},
             %{project_id: 123, user_id: 9}
           )
  end

  test "watch joins source interest and notify dispatches through the coordinator", %{
    table: table
  } do
    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

    socket = Live.watch(socket, :columns, BoardColumns, project_id: 123)
    assert socket.assigns.columns == [:todo]

    :ets.insert(table, {{:board_columns, 123}, [:todo, :doing]})

    assert :ok = Upkeep.notify(%IssueMoved{project_id: 123, issue_id: 1})

    assert_receive {:upkeep_event, %IssueMoved{project_id: 123}}

    refreshed = Live.refresh_matching(socket, %IssueMoved{project_id: 123, issue_id: 1})
    assert refreshed.assigns.columns == [:todo, :doing]
  end

  test "coordinator does not dispatch to unrelated field-indexed watchers" do
    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

    Live.watch(socket, :columns, BoardColumns, project_id: 123)

    assert :ok = Upkeep.notify(%IssueMoved{project_id: 456, issue_id: 1})

    refute_receive {:upkeep_event, %IssueMoved{project_id: 456}}
  end

  test "coordinator sends one event when a process watches overlapping source keys" do
    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

    socket = Live.watch(socket, :columns, BoardColumns, project_id: 123)
    Live.watch(socket, :my_issues, MyIssues, project_id: 123, user_id: 9)

    assert socket.assigns.columns == [:todo]

    assert :ok = Upkeep.notify(%IssueMoved{project_id: 123, assignee_id: 9, issue_id: 1})

    assert_receive {:upkeep_event, %IssueMoved{project_id: 123, assignee_id: 9}}
    refute_receive {:upkeep_event, %IssueMoved{project_id: 123, assignee_id: 9}}
  end
end
