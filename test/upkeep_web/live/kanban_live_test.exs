defmodule UpkeepWeb.KanbanLiveTest do
  use UpkeepWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  setup do
    Upkeep.Kanban.reset!()
    Upkeep.clear_events()
    :ok
  end

  test "renders the seeded board", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/")

    assert html =~ "Project board"
    assert html =~ "Shape source API"
    assert html =~ "Test durable fanout"
    assert html =~ "No issue selected"
    assert html =~ "Seeded project board"
    assert html =~ "2 open issues"
    assert html =~ "My issues (1)"
  end

  test "disconnected render loads data without registering source interest", %{conn: conn} do
    html =
      conn
      |> get(~p"/")
      |> html_response(200)

    assert html =~ "Project board"
    assert html =~ "Shape source API"

    refute Upkeep.Kanban.Sources.BoardColumns
           |> member_pids(project_id: Upkeep.Kanban.project_id())
           |> Enum.member?(self())

    assert Enum.any?(Upkeep.recent_events(), fn event ->
             event.event == [:upkeep, :source, :watch] and
               event.metadata.source == Upkeep.Kanban.Sources.BoardColumns and
               event.metadata.registered? == false
           end)
  end

  test "creating an issue refreshes board columns and activity", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    view
    |> form("form[phx-submit='create_issue']", issue: %{title: "Wire kanban board"})
    |> render_submit()

    html = assert_eventually_render(view, "Wire kanban board")
    assert html =~ "Wire kanban board"
    assert html =~ "Created Wire kanban board"
    assert html =~ "3 open issues"
  end

  test "external domain mutations refresh a connected LiveView", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/")

    refute html =~ "Created outside the LiveView"

    Upkeep.clear_events()

    assert {:ok, _issue} =
             Upkeep.Kanban.create_issue(
               Upkeep.Kanban.project_id(),
               "Created outside the LiveView"
             )

    html = assert_eventually_render(view, "Created outside the LiveView")

    assert html =~ "Created Created outside the LiveView"
    assert html =~ "3 open issues"

    reloads = recent_source_assign_events()

    assert Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.BoardColumns))
    assert Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.ProjectActivity))
  end

  test "moving and assigning an issue refreshes overlapping sources", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    view
    |> element("button[phx-click='move_issue'][phx-value-id='10'][phx-value-column='2']")
    |> render_click()

    html = assert_eventually_render(view, "Moved Shape source API")
    assert html =~ "Moved Shape source API"

    view
    |> element("button[phx-click='assign_issue'][phx-value-id='11']")
    |> render_click()

    html = assert_eventually_render(view, "Assigned Test durable fanout")
    assert html =~ "Assigned Test durable fanout"
    assert html =~ "Test durable fanout"
    assert html =~ "My issues (2)"
  end

  test "issues can move from done back to another column", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    view
    |> element("button[phx-click='move_issue'][phx-value-id='10'][phx-value-column='3']")
    |> render_click()

    assert_eventually_has_element(
      view,
      "#issue-10 button[phx-click='move_issue'][phx-value-column='1']"
    )

    assert_eventually_has_element(
      view,
      "#issue-10 button[phx-click='move_issue'][phx-value-column='2']"
    )

    view
    |> element("button[phx-click='move_issue'][phx-value-id='10'][phx-value-column='1']")
    |> render_click()

    html = assert_eventually_render(view, "Moved Shape source API")
    assert html =~ "Moved Shape source API"
    assert html =~ "2 open issues"
  end

  test "renaming an open detail refreshes board, detail title, my issues, and activity", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, ~p"/")

    view
    |> element("button[phx-click='open_issue'][phx-value-id='10']")
    |> render_click()

    Upkeep.clear_events()

    view
    |> form("#rename-issue-10", issue: %{id: "10", title: "Polish source API"})
    |> render_submit()

    html = assert_eventually_render(view, "Polish source API")
    assert html =~ "Polish source API"
    assert html =~ "Polish source API · 1 comment"
    assert html =~ "Renamed Shape source API to Polish source API"
    assert html =~ "My issues (1)"

    reloads = recent_source_assign_events()

    assert Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.BoardColumns))
    assert Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.ProjectActivity))
    assert Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.MyIssues))
    refute Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.IssueComments))
  end

  test "comments and archive refresh their watched surfaces", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    view
    |> element("button[phx-click='open_issue'][phx-value-id='10']")
    |> render_click()

    html = render(view)
    assert html =~ "Keep the source contract boring."
    assert html =~ "1 comment"

    view
    |> form("form[phx-submit='add_comment']",
      comment: %{body: "This confirms comments source refreshes."}
    )
    |> render_submit()

    html = assert_eventually_render(view, "This confirms comments source refreshes.")
    assert html =~ "This confirms comments source refreshes."
    assert html =~ "Commented on Shape source API"
    assert html =~ "2 comments"

    view
    |> element("button[phx-click='archive_issue'][phx-value-id='10']")
    |> render_click()

    html = assert_eventually_render(view, "Archived Shape source API")
    assert html =~ "Archived Shape source API"
    refute html =~ ~s(id="issue-10")
    assert html =~ ~s(id="archived-issue-10")
    assert html =~ "Archived (1)"
    assert html =~ "No issue selected"
    refute html =~ "This confirms comments source refreshes."
    assert html =~ "1 open issue"

    view
    |> element("button[phx-click='restore_issue'][phx-value-id='10'][phx-value-column='2']")
    |> render_click()

    html = assert_eventually_render(view, "Restored Shape source API")
    assert html =~ "Restored Shape source API"
    assert html =~ ~s(id="issue-10")
    refute html =~ ~s(id="archived-issue-10")
    assert html =~ "Archived (0)"
    assert html =~ "2 open issues"
  end

  test "closed issue detail removes scoped comment refreshes from connected LiveView", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, ~p"/")

    view
    |> element("button[phx-click='open_issue'][phx-value-id='10']")
    |> render_click()

    html = render(view)
    assert html =~ "Keep the source contract boring."
    assert html =~ "Shape source API · 1 comment"

    view
    |> element("button[phx-click='close_issue']")
    |> render_click()

    html = render(view)
    assert html =~ "No issue selected"

    Upkeep.clear_events()

    assert {:ok, _comment} =
             Upkeep.Kanban.add_comment(10, "This should not refresh the closed detail.")

    html = assert_eventually_render(view, "Commented on Shape source API")

    assert html =~ "No issue selected"
    refute html =~ "This should not refresh the closed detail."

    reloads = recent_source_assign_events()

    refute Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.IssueComments))
    assert Enum.any?(reloads, &source_assigned?(&1, Upkeep.Kanban.Sources.ProjectActivity))
  end

  defp member_pids(source, params) do
    params = Map.new(params)
    source_id = {source, params}

    source_id
    |> Upkeep.Coordinator.NodeDAG.subscribers()
    |> MapSet.to_list()
  end

  defp recent_source_assign_events do
    _ = :sys.get_state(Upkeep.Observability)

    Upkeep.recent_events()
    |> Enum.filter(&(&1.event == [:upkeep, :live, :assign] and &1.metadata.kind == :source))
  end

  defp source_assigned?(event, source) do
    case event.metadata.source_id do
      {^source, _params} -> true
      {:scoped, _component, {^source, _params}} -> true
      _ -> false
    end
  end

  defp assert_eventually_render(view, expected, attempts \\ 20)

  defp assert_eventually_render(view, expected, attempts) when attempts > 0 do
    html = render(view)

    if html =~ expected do
      html
    else
      Process.sleep(25)
      assert_eventually_render(view, expected, attempts - 1)
    end
  end

  defp assert_eventually_render(view, expected, 0) do
    html = render(view)
    flunk("expected rendered LiveView to include #{inspect(expected)}, got:\n#{html}")
  end

  defp assert_eventually_has_element(view, selector, attempts \\ 20)

  defp assert_eventually_has_element(view, selector, attempts) when attempts > 0 do
    if has_element?(view, selector) do
      true
    else
      Process.sleep(25)
      assert_eventually_has_element(view, selector, attempts - 1)
    end
  end

  defp assert_eventually_has_element(view, selector, 0) do
    html = render(view)
    flunk("expected rendered LiveView to include selector #{inspect(selector)}, got:\n#{html}")
  end
end
