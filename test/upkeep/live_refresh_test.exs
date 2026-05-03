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

  setup do
    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:issues, 1}, [:issue_a]})
    :ets.insert(table, {{:activity, 1}, [:activity_a]})
    :ets.insert(table, {{:failing, 1}, [:stable]})
    :ets.insert(table, {{:loads, :issues, 1}, 0})
    :ets.insert(table, {{:loads, :activity, 1}, 0})
    :ets.insert(table, {{:loads, :failing, 1}, 0})

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

  defp updated_issue(project_id, issue_id) do
    %Issue{project_id: project_id, issue_id: issue_id}
    |> Upkeep.Change.updated()
  end

  defp inserted_comment(project_id, issue_id) do
    %Comment{project_id: project_id, issue_id: issue_id}
    |> Upkeep.Change.inserted()
  end
end
