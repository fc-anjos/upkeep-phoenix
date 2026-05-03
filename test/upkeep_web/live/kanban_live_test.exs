defmodule UpkeepWeb.KanbanLiveTest do
  use UpkeepWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  setup do
    Upkeep.Kanban.reset!()
    :ok
  end

  test "renders the seeded board", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/kanban")

    assert html =~ "Project board"
    assert html =~ "Shape source API"
    assert html =~ "Test durable fanout"
    assert html =~ "Keep the source contract boring."
    assert html =~ "Seeded project board"
    assert html =~ "2 open issues"
    assert html =~ "My issues (1)"
  end

  test "creating an issue refreshes board columns and activity", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/kanban")

    view
    |> form("form[phx-submit='create_issue']", issue: %{title: "Wire kanban demo"})
    |> render_submit()

    html = render(view)
    assert html =~ "Wire kanban demo"
    assert html =~ "Created Wire kanban demo"
    assert html =~ "3 open issues"
  end

  test "moving and assigning an issue refreshes overlapping sources", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/kanban")

    view
    |> element("button[phx-click='move_issue'][phx-value-id='10']")
    |> render_click()

    html = render(view)
    assert html =~ "Moved Shape source API"

    view
    |> element("button[phx-click='assign_issue'][phx-value-id='11']")
    |> render_click()

    html = render(view)
    assert html =~ "Assigned Test durable fanout"
    assert html =~ "Test durable fanout"
    assert html =~ "My issues (2)"
  end

  test "comments and archive refresh their watched surfaces", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/kanban")

    view
    |> form("form[phx-submit='add_comment']",
      comment: %{body: "This proves comments source refreshes."}
    )
    |> render_submit()

    html = render(view)
    assert html =~ "This proves comments source refreshes."
    assert html =~ "Commented on Shape source API"

    view
    |> element("button[phx-click='archive_issue'][phx-value-id='10']")
    |> render_click()

    html = render(view)
    assert html =~ "Archived Shape source API"
    refute html =~ ~s(id="issue-10")
    assert html =~ "1 open issue"
  end
end
