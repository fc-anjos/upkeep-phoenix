defmodule Upkeep.LiveRefreshTest do
  use ExUnit.Case, async: false

  alias Upkeep.Live

  defmodule Issue do
    defstruct [:project_id, :issue_id]
  end

  defmodule Comment do
    defstruct [:project_id, :issue_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    query(fn s ->
      Upkeep.LiveRefreshTest.bump_load({:loads, :issues, s.project_id})
      Upkeep.LiveRefreshTest.table_value({:issues, s.project_id})
    end)

    invalidated_by(Issue, :updated, on: :project_id)
  end

  defmodule ProjectActivity do
    use Upkeep.Source

    query(fn s ->
      Upkeep.LiveRefreshTest.bump_load({:loads, :activity, s.project_id})
      Upkeep.LiveRefreshTest.table_value({:activity, s.project_id})
    end)

    invalidated_by(Issue, :updated, on: :project_id)
    invalidated_by(Comment, :inserted, on: :project_id)
  end

  defmodule FailingIssues do
    use Upkeep.Source

    query(fn s ->
      Upkeep.LiveRefreshTest.bump_load({:loads, :failing, s.project_id})

      case Upkeep.LiveRefreshTest.table_value({:failing, s.project_id}) do
        :raise -> raise "source failed"
        value -> value
      end
    end)

    invalidated_by(Issue, :updated, on: :project_id)
  end

  defmodule IssueComments do
    use Upkeep.Source

    query(fn s ->
      Upkeep.LiveRefreshTest.bump_load({:loads, :comments, s.issue_id})
      Upkeep.LiveRefreshTest.table_value({:comments, s.issue_id})
    end)

    invalidated_by(Comment, :inserted, on: :issue_id)
  end

  setup do
    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:issues, 1}, [:issue_a]})
    :ets.insert(table, {{:activity, 1}, [:activity_a]})
    :ets.insert(table, {{:failing, 1}, [:stable]})
    :ets.insert(table, {{:comments, 1}, [:comment_a]})
    :ets.insert(table, {{:comments, 2}, [:comment_b]})
    :ets.insert(table, {{:loads, :issues, 1}, 0})
    :ets.insert(table, {{:loads, :activity, 1}, 0})
    :ets.insert(table, {{:loads, :failing, 1}, 0})
    :ets.insert(table, {{:loads, :comments, 1}, 0})
    :ets.insert(table, {{:loads, :comments, 2}, 0})
    :ets.insert(table, {{:loads, :visible, 1}, 0})
    :ets.insert(table, {{:loads, :issue_count, 1}, 0})
    :ets.insert(table, {{:loads, :issue_label, 1}, 0})

    %{table: table}
  end

  test "many changes for one source reload once per flush", %{table: table} do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert load_count(:issues) == 1

    :ets.insert(table, {{:issues, 1}, [:issue_b]})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.queue_matching(updated_issue(1, 2))
      |> Live.queue_matching(updated_issue(1, 3))
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_b]
    assert load_count(:issues) == 2
  end

  test "different affected sources each reload once", %{table: table} do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:activity, ProjectActivity, project_id: 1)

    assert load_count(:issues) == 1
    assert load_count(:activity) == 1

    :ets.insert(table, {{:issues, 1}, [:issue_b]})
    :ets.insert(table, {{:activity, 1}, [:activity_b]})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_b]
    assert socket.assigns.activity == [:activity_b]
    assert load_count(:issues) == 2
    assert load_count(:activity) == 2
  end

  test "failed source load leaves old assign intact and clears pending refreshes", %{table: table} do
    socket =
      new_socket()
      |> Live.watch(:issues, FailingIssues, project_id: 1)

    assert socket.assigns.issues == [:stable]
    assert load_count(:failing) == 1

    :ets.insert(table, {{:failing, 1}, :raise})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:stable]
    assert load_count(:failing) == 2

    socket = Live.flush_refreshes(socket)
    assert socket.assigns.issues == [:stable]
    assert load_count(:failing) == 2
  end

  test "queues are per socket process state", %{table: table} do
    socket_a = Live.watch(new_socket(), :issues, ProjectIssues, project_id: 1)
    socket_b = Live.watch(new_socket(), :issues, ProjectIssues, project_id: 1)

    :ets.insert(table, {{:issues, 1}, [:issue_b]})

    socket_a =
      socket_a
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket_a.assigns.issues == [:issue_b]
    assert socket_b.assigns.issues == [:issue_a]
  end

  test "watch is idempotent for the same source identity" do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue_a]
    assert load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 1
  end

  test "duplicate watch can alias the same source value without duplicate membership", %{
    table: table
  } do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:other_issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue_a]
    assert socket.assigns.other_issues == [:issue_a]
    assert load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 1

    :ets.insert(table, {{:issues, 1}, [:issue_b]})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_b]
    assert socket.assigns.other_issues == [:issue_b]
    assert load_count(:issues) == 2
  end

  test "unwatch by assign keeps shared source interest until last alias is removed" do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:other_issues, ProjectIssues, project_id: 1)

    socket = Live.unwatch(socket, :other_issues)

    assert member_count(ProjectIssues, project_id: 1) == 1

    _socket = Live.unwatch(socket, :issues)

    assert member_count(ProjectIssues, project_id: 1) == 0
  end

  test "unwatch by assign leaves interest and stops refreshes", %{table: table} do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert member_count(ProjectIssues, project_id: 1) == 1

    socket = Live.unwatch(socket, :issues)

    assert member_count(ProjectIssues, project_id: 1) == 0

    :ets.insert(table, {{:issues, 1}, [:issue_b]})

    change = updated_issue(1, 1)
    assert :ok = Upkeep.notify(change)
    refute_receive {:upkeep_event, ^change}

    socket =
      socket
      |> Live.queue_matching(change)
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_a]
    assert load_count(:issues) == 1
  end

  test "unwatch by source params clears pending refreshes", %{table: table} do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.queue_matching(updated_issue(1, 1))

    :ets.insert(table, {{:issues, 1}, [:issue_b]})

    socket =
      socket
      |> Live.unwatch(ProjectIssues, project_id: 1)
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_a]
    assert load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 0
  end

  test "derived assigns recompute through a real dependency chain", %{table: table} do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], fn %{issues: issues} ->
        bump_load({:loads, :issue_count, 1})
        length(issues)
      end)
      |> Live.derive(:issue_label, [:issue_count], fn %{issue_count: count} ->
        bump_load({:loads, :issue_label, 1})
        "#{count} issue"
      end)

    assert socket.assigns.issue_count == 1
    assert socket.assigns.issue_label == "1 issue"
    assert load_count(:issue_count) == 1
    assert load_count(:issue_label) == 1

    :ets.insert(table, {{:issues, 1}, [:issue_a, :issue_b]})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_count == 2
    assert socket.assigns.issue_label == "2 issue"
    assert load_count(:issue_count) == 2
    assert load_count(:issue_label) == 2
  end

  test "shared derived intermediates compute once and unrelated sources do not trigger them", %{
    table: table
  } do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:activity, ProjectActivity, project_id: 1)
      |> Live.derive(:visible_issues, [:issues], fn %{issues: issues} ->
        bump_load({:loads, :visible, 1})
        issues
      end)
      |> Live.derive(:issue_count, [:visible_issues], fn %{visible_issues: issues} ->
        bump_load({:loads, :issue_count, 1})
        length(issues)
      end)
      |> Live.derive(:issue_label, [:visible_issues], fn %{visible_issues: issues} ->
        bump_load({:loads, :issue_label, 1})
        List.first(issues)
      end)

    assert socket.assigns.issue_count == 1
    assert socket.assigns.issue_label == :issue_a
    assert load_count(:visible) == 1
    assert load_count(:issue_count) == 1
    assert load_count(:issue_label) == 1

    :ets.insert(table, {{:activity, 1}, [:activity_b]})

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.activity == [:activity_b]
    assert load_count(:visible) == 1
    assert load_count(:issue_count) == 1
    assert load_count(:issue_label) == 1

    :ets.insert(table, {{:issues, 1}, [:issue_a, :issue_b]})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_count == 2
    assert socket.assigns.issue_label == :issue_a
    assert load_count(:visible) == 2
    assert load_count(:issue_count) == 2
    assert load_count(:issue_label) == 2
  end

  test "component-scoped sources enter and leave the graph with downstream derived nodes", %{
    table: table
  } do
    socket =
      new_socket()
      |> Live.component(:issue_detail, [], fn %{} -> %{issue_id: 1} end)
      |> Live.watch(:comments, IssueComments, [issue_id: 1], under: :issue_detail)
      |> Live.derive(:comment_count, [:comments], fn %{comments: comments} -> length(comments) end)

    assert socket.assigns.comments == [:comment_a]
    assert socket.assigns.comment_count == 1
    assert load_count(:comments, 1) == 1
    assert member_count(IssueComments, issue_id: 1) == 1

    socket = Live.remove_component(socket, :issue_detail)

    assert member_count(IssueComments, issue_id: 1) == 0

    :ets.insert(table, {{:comments, 1}, [:comment_a, :comment_c]})

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.comments == [:comment_a]
    assert socket.assigns.comment_count == 1
    assert load_count(:comments, 1) == 1

    socket =
      socket
      |> Live.component(:issue_detail, [], fn %{} -> %{issue_id: 2} end)
      |> Live.watch(:comments, IssueComments, [issue_id: 2], under: :issue_detail)
      |> Live.derive(:comment_count, [:comments], fn %{comments: comments} -> length(comments) end)

    assert socket.assigns.comments == [:comment_b]
    assert socket.assigns.comment_count == 1
    assert member_count(IssueComments, issue_id: 1) == 0
    assert member_count(IssueComments, issue_id: 2) == 1
  end

  test "removing component-scoped source preserves shared source interest", %{table: table} do
    socket =
      new_socket()
      |> Live.watch(:comments, IssueComments, issue_id: 1)
      |> Live.component(:issue_detail, [], fn %{} -> %{issue_id: 1} end)
      |> Live.watch(:detail_comments, IssueComments, [issue_id: 1], under: :issue_detail)

    assert member_count(IssueComments, issue_id: 1) == 1
    assert load_count(:comments, 1) == 2

    socket = Live.remove_component(socket, :issue_detail)

    assert member_count(IssueComments, issue_id: 1) == 1

    change = inserted_comment(1, 1)
    assert :ok = Upkeep.notify(change)
    assert_receive {:upkeep_event, ^change}

    :ets.insert(table, {{:comments, 1}, [:comment_a, :comment_c]})

    socket =
      socket
      |> Live.queue_matching(change)
      |> Live.flush_refreshes()

    assert socket.assigns.comments == [:comment_a, :comment_c]
    assert load_count(:comments, 1) == 3

    _socket = Live.unwatch(socket, :comments)

    assert member_count(IssueComments, issue_id: 1) == 0
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

  def table_value(key) do
    [{^key, value}] = :ets.lookup(__MODULE__, key)
    value
  end

  def bump_load(key) do
    :ets.update_counter(__MODULE__, key, 1)
  end

  defp load_count(source) do
    table_value({:loads, source, 1})
  end

  defp load_count(source, id) do
    table_value({:loads, source, id})
  end

  defp updated_issue(project_id, issue_id) do
    %Issue{project_id: project_id, issue_id: issue_id}
    |> Upkeep.Change.updated()
  end

  defp inserted_comment(project_id, issue_id) do
    %Comment{project_id: project_id, issue_id: issue_id}
    |> Upkeep.Change.inserted()
  end

  defp member_count(source, params) do
    params = Map.new(params)

    source.__upkeep_interest_keys__(params)
    |> hd()
    |> Upkeep.Source.group_key()
    |> then(&Group.members(Upkeep.DurableSupervisor, &1))
    |> Enum.count(fn {pid, _meta} -> pid == self() end)
  end
end
