defmodule UpkeepWeb.KanbanLive do
  use UpkeepWeb, :live_view
  use Upkeep.Live

  alias Upkeep.Kanban
  alias Upkeep.Kanban.Sources

  @project_id Kanban.project_id()
  @current_user_id Kanban.current_user_id()
  @selected_issue_id 10

  @impl true
  def mount(_params, _session, socket) do
    {:ok,
     socket
     |> assign(:new_issue_title, "")
     |> assign(:comment_body, "")
     |> assign(:selected_issue_id, @selected_issue_id)
     |> watch(:columns, Sources.BoardColumns, project_id: @project_id)
     |> watch(:activity, Sources.ProjectActivity, project_id: @project_id)
     |> watch(:my_issues, Sources.MyIssues, project_id: @project_id, user_id: @current_user_id)
     |> watch(:comments, Sources.IssueComments, issue_id: @selected_issue_id)
     |> derive(:board_stats, [:columns], &board_stats/1)
     |> derive(:board_badge, [:board_stats], &board_badge/1)
     |> derive(:my_issue_count, [:my_issues], fn %{my_issues: issues} -> length(issues) end)}
  end

  @impl true
  def handle_event("create_issue", %{"issue" => %{"title" => title}}, socket) do
    title = title |> String.trim() |> default_title()
    {:ok, _issue} = Kanban.create_issue(@project_id, title)

    {:noreply, assign(socket, :new_issue_title, "")}
  end

  def handle_event("move_issue", %{"id" => id, "column" => column_id}, socket) do
    {:ok, _issue} = Kanban.move_issue(to_integer(id), to_integer(column_id))
    {:noreply, socket}
  end

  def handle_event("assign_issue", %{"id" => id}, socket) do
    {:ok, _issue} = Kanban.assign_issue(to_integer(id), @current_user_id)
    {:noreply, socket}
  end

  def handle_event("archive_issue", %{"id" => id}, socket) do
    {:ok, _issue} = Kanban.archive_issue(to_integer(id))
    {:noreply, socket}
  end

  def handle_event("add_comment", %{"comment" => %{"body" => body}}, socket) do
    body = body |> String.trim() |> default_comment()
    {:ok, _comment} = Kanban.add_comment(socket.assigns.selected_issue_id, body)

    {:noreply, assign(socket, :comment_body, "")}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <main class="min-h-screen bg-zinc-100 text-zinc-950">
      <div class="mx-auto flex max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:px-8">
        <header class="flex flex-col gap-3 border-b border-zinc-300 pb-4 md:flex-row md:items-end md:justify-between">
          <div>
            <p class="text-sm font-semibold text-zinc-500">Upkeep demo</p>
            <h1 class="text-2xl font-bold tracking-normal">Project board</h1>
            <p class="mt-1 text-sm text-zinc-600">
              {@board_badge} · {@board_stats.assigned_count} assigned
            </p>
          </div>

          <.form for={%{}} as={:issue} phx-submit="create_issue" class="flex w-full gap-2 md:max-w-md">
            <input
              name="issue[title]"
              value={@new_issue_title}
              placeholder="New issue"
              class="min-w-0 flex-1 rounded border border-zinc-300 bg-white px-3 py-2 text-sm"
            />
            <button class="inline-flex items-center gap-2 rounded bg-zinc-900 px-3 py-2 text-sm font-semibold text-white">
              <.icon name="hero-plus" class="size-4" /> Add
            </button>
          </.form>
        </header>

        <section class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_20rem]">
          <div class="grid gap-3 md:grid-cols-3">
            <article
              :for={column <- @columns}
              class="min-h-96 rounded border border-zinc-300 bg-zinc-50"
            >
              <header class="flex items-center justify-between border-b border-zinc-300 px-3 py-2">
                <h2 class="text-sm font-bold">{column.name}</h2>
                <span class="rounded bg-zinc-200 px-2 py-1 text-xs font-semibold">
                  {length(column.issues)}
                </span>
              </header>

              <div class="flex flex-col gap-2 p-2">
                <div
                  :for={issue <- column.issues}
                  id={"issue-#{issue.id}"}
                  class="rounded border border-zinc-300 bg-white p-3 shadow-sm"
                >
                  <div class="flex items-start justify-between gap-2">
                    <h3 class="text-sm font-semibold">{issue.title}</h3>
                    <span
                      :if={issue.assignee_id}
                      class="rounded bg-emerald-100 px-2 py-1 text-xs font-semibold text-emerald-800"
                    >
                      Me
                    </span>
                  </div>

                  <div class="mt-3 flex flex-wrap gap-1">
                    <button
                      :if={column.id != 3}
                      phx-click="move_issue"
                      phx-value-id={issue.id}
                      phx-value-column={column.id + 1}
                      class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold"
                    >
                      <.icon name="hero-arrow-right" class="size-3" /> Move
                    </button>
                    <button
                      phx-click="assign_issue"
                      phx-value-id={issue.id}
                      class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold"
                    >
                      <.icon name="hero-user-plus" class="size-3" /> Assign
                    </button>
                    <button
                      phx-click="archive_issue"
                      phx-value-id={issue.id}
                      class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold"
                    >
                      <.icon name="hero-archive-box" class="size-3" /> Archive
                    </button>
                  </div>
                </div>
              </div>
            </article>
          </div>

          <aside class="flex flex-col gap-3">
            <section class="rounded border border-zinc-300 bg-white">
              <header class="border-b border-zinc-300 px-3 py-2">
                <h2 class="text-sm font-bold">My issues ({@my_issue_count})</h2>
              </header>
              <ul class="divide-y divide-zinc-200 text-sm">
                <li :for={issue <- @my_issues} class="px-3 py-2">{issue.title}</li>
              </ul>
            </section>

            <section class="rounded border border-zinc-300 bg-white">
              <header class="border-b border-zinc-300 px-3 py-2">
                <h2 class="text-sm font-bold">Issue comments</h2>
              </header>
              <ul class="divide-y divide-zinc-200 text-sm">
                <li :for={comment <- @comments} class="px-3 py-2">{comment.body}</li>
              </ul>
              <.form
                for={%{}}
                as={:comment}
                phx-submit="add_comment"
                class="flex gap-2 border-t border-zinc-300 p-2"
              >
                <input
                  name="comment[body]"
                  value={@comment_body}
                  placeholder="Comment"
                  class="min-w-0 flex-1 rounded border border-zinc-300 px-2 py-1 text-sm"
                />
                <button class="inline-flex items-center gap-1 rounded bg-zinc-900 px-2 py-1 text-sm font-semibold text-white">
                  <.icon name="hero-chat-bubble-left" class="size-4" /> Add
                </button>
              </.form>
            </section>

            <section class="rounded border border-zinc-300 bg-white">
              <header class="border-b border-zinc-300 px-3 py-2">
                <h2 class="text-sm font-bold">Activity</h2>
              </header>
              <ol class="divide-y divide-zinc-200 text-sm">
                <li :for={entry <- @activity} class="px-3 py-2">{entry.message}</li>
              </ol>
            </section>
          </aside>
        </section>
      </div>
    </main>
    """
  end

  defp default_title(""), do: "Untitled issue"
  defp default_title(title), do: title

  defp default_comment(""), do: "No details yet."
  defp default_comment(body), do: body

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
