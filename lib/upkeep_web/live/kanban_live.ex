defmodule UpkeepWeb.KanbanLive do
  use UpkeepWeb, :live_view
  use Upkeep.Live

  alias Upkeep.Kanban
  alias Upkeep.Kanban.Sources
  alias UpkeepWeb.KanbanComponents

  @project_id Kanban.project_id()
  @current_user_id Kanban.current_user_id()

  @impl true
  def mount(_params, _session, socket) do
    {:ok,
     socket
     |> assign(:new_issue_title, "")
     |> assign(:new_issue_form, issue_form())
     |> assign(:comment_body, "")
     |> assign(:comment_form, comment_form())
     |> assign(:selected_issue_id, nil)
     |> assign(:selected_issue_title, nil)
     |> assign(:comments, [])
     |> assign(:comment_count, 0)
     |> watch(:columns, Sources.BoardColumns, project_id: @project_id)
     |> watch(:activity, Sources.ProjectActivity, project_id: @project_id)
     |> watch(:my_issues, Sources.MyIssues, project_id: @project_id, user_id: @current_user_id)
     |> watch(:archived_issues, Sources.ArchivedIssues, project_id: @project_id)
     |> derive(:board_stats, [:columns], &board_stats/1)
     |> derive(:board_badge, [:board_stats], &board_badge/1)
     |> derive(:my_issue_count, [:my_issues], fn %{my_issues: issues} -> length(issues) end)
     |> derive(:archived_issue_count, [:archived_issues], fn %{archived_issues: issues} ->
       length(issues)
     end)}
  end

  @impl true
  def handle_event("create_issue", %{"issue" => %{"title" => title}}, socket) do
    title = title |> String.trim() |> default_title()
    {:ok, _issue} = Kanban.create_issue(@project_id, title)

    {:noreply, assign(socket, new_issue_title: "", new_issue_form: issue_form())}
  end

  def handle_event("move_issue", %{"id" => id, "column" => column_id}, socket) do
    {:ok, _issue} = Kanban.move_issue(to_integer(id), to_integer(column_id))
    {:noreply, socket}
  end

  def handle_event("assign_issue", %{"id" => id}, socket) do
    {:ok, _issue} = Kanban.assign_issue(to_integer(id), @current_user_id)
    {:noreply, socket}
  end

  def handle_event("rename_issue", %{"issue" => %{"id" => id, "title" => title}}, socket) do
    {:ok, _issue} = Kanban.rename_issue(to_integer(id), title |> String.trim() |> default_title())
    {:noreply, socket}
  end

  def handle_event("open_issue", %{"id" => id}, socket) do
    {:noreply, open_issue_detail(socket, to_integer(id))}
  end

  def handle_event("close_issue", _params, socket) do
    {:noreply, close_issue_detail(socket)}
  end

  def handle_event("archive_issue", %{"id" => id}, socket) do
    {:ok, issue} = Kanban.archive_issue(to_integer(id))

    socket =
      if socket.assigns.selected_issue_id == issue.id,
        do: close_issue_detail(socket),
        else: socket

    {:noreply, socket}
  end

  def handle_event("restore_issue", %{"id" => id, "column" => column_id}, socket) do
    {:ok, _issue} = Kanban.restore_issue(to_integer(id), to_integer(column_id))
    {:noreply, socket}
  end

  def handle_event(
        "add_comment",
        %{"comment" => %{"body" => _body}},
        %{assigns: %{selected_issue_id: nil}} = socket
      ) do
    {:noreply, assign(socket, comment_body: "", comment_form: comment_form())}
  end

  def handle_event("add_comment", %{"comment" => %{"body" => body}}, socket) do
    body = body |> String.trim() |> default_comment()
    {:ok, _comment} = Kanban.add_comment(socket.assigns.selected_issue_id, body)

    {:noreply, assign(socket, comment_body: "", comment_form: comment_form())}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <KanbanComponents.board {assigns} />
    </Layouts.app>
    """
  end

  defp default_title(""), do: "Untitled issue"
  defp default_title(title), do: title

  defp default_comment(""), do: "No details yet."
  defp default_comment(body), do: body

  defp issue_form(params \\ %{"title" => ""}) do
    to_form(params, as: :issue)
  end

  defp comment_form(params \\ %{"body" => ""}) do
    to_form(params, as: :comment)
  end

  defp open_issue_detail(socket, issue_id) do
    issue = find_issue(socket.assigns.columns, issue_id)

    socket
    |> close_issue_detail()
    |> assign(:selected_issue_id, issue_id)
    |> assign(:selected_issue_title, issue_title(issue, issue_id))
    |> assign(:comment_body, "")
    |> component(:issue_detail, [:columns], fn %{columns: columns} ->
      issue = find_issue(columns, issue_id)
      %{selected_issue_title: issue_title(issue, issue_id)}
    end)
    |> watch(:comments, Sources.IssueComments, [issue_id: issue_id], under: :issue_detail)
    |> derive(:comment_count, [:comments], fn %{comments: comments} -> length(comments) end)
  end

  defp close_issue_detail(socket) do
    socket
    |> remove_component(:issue_detail)
    |> assign(:selected_issue_id, nil)
    |> assign(:selected_issue_title, nil)
    |> assign(:comments, [])
    |> assign(:comment_count, 0)
    |> assign(:comment_body, "")
    |> assign(:comment_form, comment_form())
  end

  defp find_issue(columns, issue_id) do
    columns
    |> Enum.flat_map(& &1.issues)
    |> Enum.find(&(&1.id == issue_id))
  end

  defp issue_title(nil, issue_id), do: "Issue #{issue_id}"
  defp issue_title(issue, _issue_id), do: issue.title

  defp to_integer(value) when is_binary(value), do: String.to_integer(value)
  defp to_integer(value) when is_integer(value), do: value

  defp board_stats(%{columns: columns}) do
    issues = Enum.flat_map(columns, & &1.issues)

    %{
      open_count: length(issues),
      assigned_count: Enum.count(issues, & &1.assignee_id)
    }
  end

  defp board_badge(%{board_stats: %{open_count: 1}}), do: "1 open issue"
  defp board_badge(%{board_stats: %{open_count: count}}), do: "#{count} open issues"
end
