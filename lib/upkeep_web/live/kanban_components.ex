defmodule UpkeepWeb.KanbanComponents do
  @moduledoc false

  use UpkeepWeb, :html

  def board(assigns) do
    ~H"""
    <div class="mx-auto flex max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:px-8">
      <.board_header
        new_issue_form={@new_issue_form}
        board_badge={@board_badge}
        board_stats={@board_stats}
      />

      <section class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_20rem]">
        <div
          id="client-owned-kanban-state"
          phx-update="ignore"
          phx-hook="UpkeepClientState"
          aria-hidden="true"
          style="position:absolute;width:0;height:0;overflow:hidden;"
        >
          <span data-client-owned-value="">server-rendered</span>
        </div>

        <.columns columns={@columns} />

        <aside class="flex flex-col gap-3">
          <.my_issues issues={@my_issues} count={@my_issue_count} />
          <.archived_issues
            issues={@archived_issues}
            columns={@columns}
            count={@archived_issue_count}
          />
          <.comments
            selected_issue_id={@selected_issue_id}
            selected_issue_title={@selected_issue_title}
            comment_count={@comment_count}
            comments={@comments}
            comment_form={@comment_form}
          />
          <.activity entries={@activity} />
        </aside>
      </section>
    </div>
    """
  end

  def board_header(assigns) do
    ~H"""
    <header class="flex flex-col gap-3 border-b border-zinc-300 pb-4 dark:border-zinc-700 md:flex-row md:items-end md:justify-between">
      <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between md:flex-1">
        <div>
          <p class="text-sm font-semibold text-zinc-500 dark:text-zinc-400">
            Upkeep reference app
          </p>
          <h1 class="text-2xl font-bold tracking-normal">Project board</h1>
          <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">
            {@board_badge} · {@board_stats.assigned_count} assigned
          </p>
        </div>

        <.theme_toggle />
      </div>

      <.form
        for={@new_issue_form}
        id="new-issue-form"
        phx-submit="create_issue"
        class="flex w-full gap-2 md:max-w-md"
      >
        <.input
          field={@new_issue_form[:title]}
          type="text"
          placeholder="New issue"
          class="min-w-0 flex-1 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-900"
        />
        <button class="inline-flex items-center gap-2 rounded bg-zinc-900 px-3 py-2 text-sm font-semibold text-white dark:bg-zinc-100 dark:text-zinc-950">
          <.icon name="hero-plus" class="size-4" /> Add
        </button>
      </.form>
    </header>
    """
  end

  def columns(assigns) do
    ~H"""
    <div class="grid gap-3 md:grid-cols-3">
      <article
        :for={column <- @columns}
        class="min-h-96 rounded border border-zinc-300 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900"
      >
        <header class="flex items-center justify-between border-b border-zinc-300 px-3 py-2 dark:border-zinc-700">
          <h2 class="text-sm font-bold">{column.name}</h2>
          <span class="rounded bg-zinc-200 px-2 py-1 text-xs font-semibold dark:bg-zinc-800">
            {length(column.issues)}
          </span>
        </header>

        <div class="flex flex-col gap-2 p-2">
          <.issue_card :for={issue <- column.issues} issue={issue} column={column} columns={@columns} />
        </div>
      </article>
    </div>
    """
  end

  def issue_card(assigns) do
    ~H"""
    <div
      id={"issue-#{@issue.id}"}
      class="rounded border border-zinc-300 bg-white p-3 shadow-sm dark:border-zinc-700 dark:bg-zinc-950"
    >
      <div class="flex items-start justify-between gap-2">
        <h3 class="text-sm font-semibold">{@issue.title}</h3>
        <span
          :if={@issue.assignee_id}
          class="rounded bg-emerald-100 px-2 py-1 text-xs font-semibold text-emerald-800"
        >
          Me
        </span>
      </div>

      <.form
        for={%{}}
        as={:issue}
        id={"rename-issue-#{@issue.id}"}
        phx-submit="rename_issue"
        class="mt-3 flex gap-1"
      >
        <.input field={rename_issue_form(@issue)[:id]} type="hidden" id={"issue-id-#{@issue.id}"} />
        <.input
          field={rename_issue_form(@issue)[:title]}
          type="text"
          id={"issue-title-#{@issue.id}"}
          aria-label="Issue title"
          class="min-w-0 flex-1 rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-700 dark:bg-zinc-900"
        />
        <button class="inline-flex size-7 items-center justify-center rounded border border-zinc-300 dark:border-zinc-700">
          <.icon name="hero-check" class="size-3" />
        </button>
      </.form>

      <div class="mt-3 flex flex-wrap gap-1">
        <button
          :for={target_column <- move_targets(@columns, @column.id)}
          phx-click="move_issue"
          phx-value-id={@issue.id}
          phx-value-column={target_column.id}
          class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold dark:border-zinc-700"
        >
          <.icon name={move_icon(@column.position, target_column.position)} class="size-3" />
          {target_column.name}
        </button>
        <button
          phx-click="assign_issue"
          phx-value-id={@issue.id}
          class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold dark:border-zinc-700"
        >
          <.icon name="hero-user-plus" class="size-3" /> Assign
        </button>
        <button
          phx-click="open_issue"
          phx-value-id={@issue.id}
          class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold dark:border-zinc-700"
        >
          <.icon name="hero-eye" class="size-3" /> Details
        </button>
        <button
          phx-click="archive_issue"
          phx-value-id={@issue.id}
          class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold dark:border-zinc-700"
        >
          <.icon name="hero-archive-box" class="size-3" /> Archive
        </button>
      </div>
    </div>
    """
  end

  def my_issues(assigns) do
    ~H"""
    <section class="rounded border border-zinc-300 bg-white dark:border-zinc-700 dark:bg-zinc-900">
      <header class="border-b border-zinc-300 px-3 py-2 dark:border-zinc-700">
        <h2 class="text-sm font-bold">My issues ({@count})</h2>
      </header>
      <ul class="divide-y divide-zinc-200 text-sm dark:divide-zinc-800">
        <li :for={issue <- @issues} class="px-3 py-2">{issue.title}</li>
      </ul>
    </section>
    """
  end

  def archived_issues(assigns) do
    ~H"""
    <section
      id="archived-issues-panel"
      class="rounded border border-zinc-300 bg-white dark:border-zinc-700 dark:bg-zinc-900"
    >
      <header class="flex items-center justify-between border-b border-zinc-300 px-3 py-2 dark:border-zinc-700">
        <h2 class="text-sm font-bold">Archived ({@count})</h2>
        <span class="rounded bg-zinc-100 px-2 py-1 text-xs font-semibold text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">
          Hidden
        </span>
      </header>
      <div class="divide-y divide-zinc-200 text-sm dark:divide-zinc-800">
        <div :for={issue <- @issues} id={"archived-issue-#{issue.id}"} class="px-3 py-2">
          <div class="flex items-start justify-between gap-2">
            <p class="font-semibold text-zinc-800 dark:text-zinc-100">{issue.title}</p>
            <span class="rounded bg-zinc-100 px-2 py-1 text-xs font-semibold text-zinc-500 dark:bg-zinc-800 dark:text-zinc-300">
              Archived
            </span>
          </div>
          <div class="mt-2 flex flex-wrap gap-1">
            <button
              :for={column <- @columns}
              phx-click="restore_issue"
              phx-value-id={issue.id}
              phx-value-column={column.id}
              class="inline-flex items-center gap-1 rounded border border-zinc-300 px-2 py-1 text-xs font-semibold dark:border-zinc-700"
            >
              <.icon name="hero-arrow-uturn-left" class="size-3" /> {column.name}
            </button>
          </div>
        </div>
        <p :if={@issues == []} class="px-3 py-4 text-sm text-zinc-500">
          No archived issues
        </p>
      </div>
    </section>
    """
  end

  def comments(assigns) do
    ~H"""
    <section class="rounded border border-zinc-300 bg-white dark:border-zinc-700 dark:bg-zinc-900">
      <header class="flex items-start justify-between gap-3 border-b border-zinc-300 px-3 py-2 dark:border-zinc-700">
        <div>
          <h2 class="text-sm font-bold">Issue comments</h2>
          <p :if={@selected_issue_title} class="mt-0.5 text-xs text-zinc-500 dark:text-zinc-400">
            {@selected_issue_title} · {comment_badge(@comment_count)}
          </p>
        </div>
        <button
          :if={@selected_issue_id}
          type="button"
          phx-click="close_issue"
          class="inline-flex size-7 items-center justify-center rounded border border-zinc-300 text-zinc-600 dark:border-zinc-700 dark:text-zinc-300"
          aria-label="Close issue"
        >
          <.icon name="hero-x-mark" class="size-4" />
        </button>
      </header>
      <div :if={@selected_issue_id}>
        <ul class="divide-y divide-zinc-200 text-sm dark:divide-zinc-800">
          <li :for={comment <- @comments} class="px-3 py-2">{comment.body}</li>
          <li :if={@comments == []} class="px-3 py-2 text-zinc-500">No comments</li>
        </ul>
        <.form
          for={@comment_form}
          id="comment-form"
          phx-submit="add_comment"
          class="flex gap-2 border-t border-zinc-300 p-2 dark:border-zinc-700"
        >
          <.input
            field={@comment_form[:body]}
            type="text"
            placeholder="Comment"
            class="min-w-0 flex-1 rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-700 dark:bg-zinc-950"
          />
          <button class="inline-flex items-center gap-1 rounded bg-zinc-900 px-2 py-1 text-sm font-semibold text-white dark:bg-zinc-100 dark:text-zinc-950">
            <.icon name="hero-chat-bubble-left" class="size-4" /> Add
          </button>
        </.form>
      </div>
      <p :if={is_nil(@selected_issue_id)} class="px-3 py-6 text-sm text-zinc-500">
        No issue selected
      </p>
    </section>
    """
  end

  def activity(assigns) do
    ~H"""
    <section class="rounded border border-zinc-300 bg-white dark:border-zinc-700 dark:bg-zinc-900">
      <header class="border-b border-zinc-300 px-3 py-2 dark:border-zinc-700">
        <h2 class="text-sm font-bold">Activity</h2>
      </header>
      <ol class="divide-y divide-zinc-200 text-sm dark:divide-zinc-800">
        <li :for={entry <- @entries} class="px-3 py-2">{entry.message}</li>
      </ol>
    </section>
    """
  end

  def theme_toggle(assigns) do
    ~H"""
    <div
      class="grid w-fit grid-cols-3 overflow-hidden rounded border border-zinc-300 bg-white text-zinc-600 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
      aria-label="Theme"
    >
      <button
        type="button"
        class="inline-flex size-9 items-center justify-center hover:bg-zinc-100 dark:hover:bg-zinc-800"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="system"
        aria-label="Use system theme"
      >
        <.icon name="hero-computer-desktop-micro" class="size-4" />
      </button>
      <button
        type="button"
        class="inline-flex size-9 items-center justify-center border-x border-zinc-300 hover:bg-zinc-100 dark:border-zinc-700 dark:hover:bg-zinc-800"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="light"
        aria-label="Use light theme"
      >
        <.icon name="hero-sun-micro" class="size-4" />
      </button>
      <button
        type="button"
        class="inline-flex size-9 items-center justify-center hover:bg-zinc-100 dark:hover:bg-zinc-800"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="dark"
        aria-label="Use dark theme"
      >
        <.icon name="hero-moon-micro" class="size-4" />
      </button>
    </div>
    """
  end

  defp rename_issue_form(issue) do
    to_form(%{"id" => issue.id, "title" => issue.title}, as: :issue)
  end

  defp move_targets(columns, current_column_id) do
    Enum.reject(columns, &(&1.id == current_column_id))
  end

  defp move_icon(current_position, target_position) when target_position < current_position,
    do: "hero-arrow-left"

  defp move_icon(_current_position, _target_position), do: "hero-arrow-right"

  defp comment_badge(1), do: "1 comment"
  defp comment_badge(count), do: "#{count} comments"
end
