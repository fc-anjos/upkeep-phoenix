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

  defmodule ScopedIssues do
    use Upkeep.Source

    query(fn s ->
      Upkeep.LiveRefreshTest.bump_load({:loads, :scoped_issues, s.user_id})
      Upkeep.LiveRefreshTest.table_value({:scoped_issues, s.user_id})
    end)

    invalidated_by(Issue, :updated, on: :issue_id, as: :user_id)
  end

  defmodule ScopedActivity do
    use Upkeep.Source

    query(fn s ->
      Upkeep.LiveRefreshTest.bump_load({:loads, :scoped_activity, s.user_id})
      Upkeep.LiveRefreshTest.table_value({:scoped_activity, s.user_id})
    end)

    invalidated_by(Comment, :inserted, on: :issue_id, as: :user_id)
  end

  defmodule BlockingScopedIssues do
    use Upkeep.Source

    query(fn s ->
      send(s.test_pid, {:blocking_load_started, self()})

      receive do
        :continue -> :ok
      after
        1_000 -> raise "blocking source was not released"
      end

      Upkeep.LiveRefreshTest.bump_load({:loads, :scoped_issues, s.user_id})
      Upkeep.LiveRefreshTest.table_value({:scoped_issues, s.user_id})
    end)

    invalidated_by(Issue, :updated, on: :issue_id, as: :user_id)
  end

  setup do
    Upkeep.Test.reset_graph()

    if :ets.info(__MODULE__) != :undefined do
      :ets.delete(__MODULE__)
    end

    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:issues, 1}, [:issue_a]})
    :ets.insert(table, {{:activity, 1}, [:activity_a]})
    :ets.insert(table, {{:failing, 1}, [:stable]})
    :ets.insert(table, {{:comments, 1}, [:comment_a]})
    :ets.insert(table, {{:comments, 2}, [:comment_b]})
    :ets.insert(table, {{:scoped_issues, 1}, [:user_1_issue]})
    :ets.insert(table, {{:scoped_issues, 2}, [:user_2_issue]})
    :ets.insert(table, {{:loads, :issues, 1}, 0})
    :ets.insert(table, {{:loads, :activity, 1}, 0})
    :ets.insert(table, {{:loads, :failing, 1}, 0})
    :ets.insert(table, {{:loads, :comments, 1}, 0})
    :ets.insert(table, {{:loads, :comments, 2}, 0})
    :ets.insert(table, {{:loads, :scoped_issues, 1}, 0})
    :ets.insert(table, {{:loads, :scoped_issues, 2}, 0})
    :ets.insert(table, {{:loads, :scoped_activity, 1}, 0})
    :ets.insert(table, {{:loads, :scoped_activity, 2}, 0})
    :ets.insert(table, {{:loads, :visible, 1}, 0})
    :ets.insert(table, {{:loads, :issue_count, 1}, 0})
    :ets.insert(table, {{:loads, :issue_label, 1}, 0})

    on_exit(fn ->
      if :ets.info(__MODULE__) != :undefined do
        :ets.delete(__MODULE__)
      end
    end)

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

  test "concurrent connected watches share the in-flight initial load for the same source identity",
       %{
         table: table
       } do
    test_pid = self()
    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [:user_issue])

    task_a =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    assert_receive {:blocking_load_started, loader_pid}

    task_b =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    refute_receive {:blocking_load_started, _second_loader_pid}, 50
    send(loader_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.issues == [:user_issue]
    assert socket_b.assigns.issues == [:user_issue]
    assert load_count(:scoped_issues, user_id) == 1
  end

  test "connected watches do not share initial values across different source params", %{
    table: table
  } do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    put_scoped_user(table, user_a, [:user_a_issue])
    put_scoped_user(table, user_b, [:user_b_issue])

    socket_a =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)

    socket_b =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)

    assert socket_a.assigns.issues == [:user_a_issue]
    assert socket_b.assigns.issues == [:user_b_issue]
    assert load_count(:scoped_issues, user_a) == 1
    assert load_count(:scoped_issues, user_b) == 1
  end

  test "connected source sharing emits initial-load miss and hit telemetry", %{table: table} do
    attach_telemetry([
      [:upkeep, :graph, :initial_load, :miss],
      [:upkeep, :graph, :initial_load, :hit]
    ])

    test_pid = self()
    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [:user_issue])
    source_id = {BlockingScopedIssues, %{test_pid: test_pid, user_id: user_id}}

    task_a =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    assert_receive {:blocking_load_started, loader_pid}

    task_b =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    refute_receive {:blocking_load_started, _second_loader_pid}, 50
    send(loader_pid, :continue)

    Task.await(task_a)
    Task.await(task_b)

    assert_receive {:telemetry, [:upkeep, :graph, :initial_load, :miss], %{count: 1},
                    %{
                      node_id: ^source_id,
                      source: BlockingScopedIssues,
                      params: %{test_pid: _, user_id: ^user_id},
                      sharing_partition: %{user_id: ^user_id}
                    }}

    assert_receive {:telemetry, [:upkeep, :graph, :initial_load, :hit], %{count: 1},
                    %{
                      node_id: ^source_id,
                      source: BlockingScopedIssues,
                      params: %{test_pid: _, user_id: ^user_id},
                      sharing_partition: %{user_id: ^user_id}
                    }}
  end

  test "concurrent connected derives share the in-flight initial compute for the same source identity",
       %{
         table: table
       } do
    attach_telemetry([
      [:upkeep, :graph, :derived_initial, :miss],
      [:upkeep, :graph, :derived_initial, :hit]
    ])

    test_pid = self()
    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [{user_id, :user_issue}])
    :ets.insert(table, {{:loads, :shared_issue_count, user_id}, 0})
    :ets.insert(table, {{:derive_test_pid, user_id}, test_pid})
    source_id = {ScopedIssues, %{user_id: user_id}}

    graph_node_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id],
       {__MODULE__, :shared_issue_count, 1}}

    task_a =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)
      end)

    assert_receive {:derived_compute_started, loader_pid, ^user_id}

    task_b =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)
      end)

    refute_receive {:derived_compute_started, _second_loader_pid, ^user_id}, 50
    send(loader_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.issue_count == 1
    assert socket_b.assigns.issue_count == 1
    assert load_count(:shared_issue_count, user_id) == 1

    assert_receive {:telemetry, [:upkeep, :graph, :derived_initial, :miss], %{count: 1},
                    %{
                      node_id: ^graph_node_id,
                      assign_name: :issue_count,
                      view: UpkeepWeb.KanbanLive,
                      dep_node_ids: [^source_id],
                      sharing_partition: %{user_id: ^user_id},
                      dep_partitions: [{^source_id, %{user_id: ^user_id}}],
                      fun: {__MODULE__, :shared_issue_count, 1}
                    }}

    assert_receive {:telemetry, [:upkeep, :graph, :derived_initial, :hit], %{count: 1},
                    %{
                      node_id: ^graph_node_id,
                      assign_name: :issue_count,
                      view: UpkeepWeb.KanbanLive,
                      dep_node_ids: [^source_id],
                      sharing_partition: %{user_id: ^user_id},
                      dep_partitions: [{^source_id, %{user_id: ^user_id}}],
                      fun: {__MODULE__, :shared_issue_count, 1}
                    }}
  end

  test "connected derived sharing does not leak values across source params", %{table: table} do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    put_scoped_user(table, user_a, [{user_a, :user_a_issue}])
    put_scoped_user(table, user_b, [{user_b, :user_b_issue}])
    :ets.insert(table, {{:loads, :shared_issue_names, user_a}, 0})
    :ets.insert(table, {{:loads, :shared_issue_names, user_b}, 0})

    socket_a =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.derive(:issue_names, [:issues], &__MODULE__.shared_issue_names/1)

    socket_b =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)
      |> Live.derive(:issue_names, [:issues], &__MODULE__.shared_issue_names/1)

    assert socket_a.assigns.issue_names == [:user_a_issue]
    assert socket_b.assigns.issue_names == [:user_b_issue]
    assert load_count(:shared_issue_names, user_a) == 1
    assert load_count(:shared_issue_names, user_b) == 1
  end

  test "derive sharing emits diagnostics for shared and local decisions", %{table: table} do
    attach_telemetry([[:upkeep, :derive, :sharing]])

    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [{user_id, :user_issue}])
    :ets.insert(table, {{:loads, :shared_issue_count, user_id}, 0})

    shared_source_id = {ScopedIssues, %{user_id: user_id}}

    shared_graph_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [shared_source_id],
       {__MODULE__, :shared_issue_count, 1}}

    connected_live_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :issue_count,
                      result: :shared,
                      reason: :shareable,
                      graph_node_id: ^shared_graph_id,
                      dep_node_ids: [{:source, ^shared_source_id}],
                      graph_dep_node_ids: [^shared_source_id],
                      sharing_partition: %{user_id: ^user_id},
                      dep_partitions: [{^shared_source_id, %{user_id: ^user_id}}],
                      fun: {__MODULE__, :shared_issue_count, 1}
                    }}

    disconnected_live_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :issue_count,
                      result: :local,
                      reason: :disconnected_socket
                    }}

    issue_count_fn = fn %{issues: issues} -> length(issues) + user_id end

    connected_live_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:issue_count, [:issues], issue_count_fn)

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :issue_count,
                      result: :local,
                      reason: :captured_fun
                    }}

    connected_live_socket()
    |> Live.component(:issue_detail, [], fn %{} -> %{issue_id: user_id} end)
    |> Live.watch(:issues, ScopedIssues, [user_id: user_id], under: :issue_detail)
    |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :issue_count,
                      result: :local,
                      reason: :component_scoped_dep
                    }}

    local_marker = :local

    connected_live_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:local_issues, [:issues], fn %{issues: issues} ->
      {local_marker, issues} |> elem(1)
    end)
    |> Live.derive(:issue_count, [:local_issues], &__MODULE__.shared_local_issue_count/1)

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :local_issues,
                      result: :local,
                      reason: :local_fun
                    }}

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :issue_count,
                      result: :local,
                      reason: :local_only_dep
                    }}
  end

  test "private derives receive current_scope implicitly and recompute when it changes", %{
    table: table
  } do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    :ets.insert(table, {{:issues, user_a}, [:user_a_issue]})
    :ets.insert(table, {{:loads, :issues, user_a}, 0})

    socket =
      connected_live_socket(%{user_id: user_a})
      |> Live.watch(:issues, ProjectIssues, project_id: user_a)
      |> Live.derive(:viewer_issue_count, [:issues], fn %{
                                                          issues: issues,
                                                          current_scope: current_scope
                                                        } ->
        {current_scope.user_id, length(issues)}
      end)

    assert socket.assigns.viewer_issue_count == {user_a, 1}

    socket =
      socket
      |> Phoenix.Component.assign(:current_scope, %{user_id: user_b})
      |> Live.flush_refreshes()

    assert socket.assigns.viewer_issue_count == {user_b, 1}
  end

  test "capturing socket scope raises under strict policy before sharing data", %{table: table} do
    with_captured_scope_policy(:raise, fn ->
      user_id = System.unique_integer([:positive])
      :ets.insert(table, {{:issues, user_id}, [:user_issue]})
      :ets.insert(table, {{:loads, :issues, user_id}, 0})
      :ets.insert(table, {{:loads, :captured_scope_label, user_id}, 0})

      socket =
        connected_live_socket(%{user_id: user_id})
        |> Live.watch(:issues, ProjectIssues, project_id: user_id)

      assert_raise Upkeep.ImplicitScopeError, ~r/captures :socket/, fn ->
        Live.derive(socket, :captured_scope_label, [:issues], fn %{issues: issues} ->
          bump_load({:loads, :captured_scope_label, user_id})
          {socket.assigns.current_scope.user_id, issues}
        end)
      end

      assert load_count(:captured_scope_label, user_id) == 0
    end)
  end

  test "capturing socket scope stays local with error telemetry in production policy", %{
    table: table
  } do
    with_captured_scope_policy(:telemetry, fn ->
      attach_telemetry([[:upkeep, :derive, :sharing]])

      user_a = System.unique_integer([:positive])
      user_b = System.unique_integer([:positive])
      :ets.insert(table, {{:issues, user_a}, [:shared_project_issue]})
      :ets.insert(table, {{:loads, :issues, user_a}, 0})
      :ets.insert(table, {{:loads, :captured_scope_label, user_a}, 0})
      :ets.insert(table, {{:loads, :captured_scope_label, user_b}, 0})

      base_a =
        connected_live_socket(%{user_id: user_a})
        |> Live.watch(:issues, ProjectIssues, project_id: user_a)

      base_b =
        connected_live_socket(%{user_id: user_b})
        |> Live.watch(:issues, ProjectIssues, project_id: user_a)

      socket_a =
        Live.derive(base_a, :captured_scope_label, [:issues], fn %{issues: issues} ->
          current_user_id = base_a.assigns.current_scope.user_id
          bump_load({:loads, :captured_scope_label, current_user_id})
          {current_user_id, issues}
        end)

      socket_b =
        Live.derive(base_b, :captured_scope_label, [:issues], fn %{issues: issues} ->
          current_user_id = base_b.assigns.current_scope.user_id
          bump_load({:loads, :captured_scope_label, current_user_id})
          {current_user_id, issues}
        end)

      assert socket_a.assigns.captured_scope_label == {user_a, [:shared_project_issue]}
      assert socket_b.assigns.captured_scope_label == {user_b, [:shared_project_issue]}
      assert load_count(:captured_scope_label, user_a) == 1
      assert load_count(:captured_scope_label, user_b) == 1

      assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                      %{
                        assign_name: :captured_scope_label,
                        result: :local,
                        reason: :captured_scope,
                        severity: :error,
                        scope_capture: :socket
                      }}

      assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                      %{
                        assign_name: :captured_scope_label,
                        result: :local,
                        reason: :captured_scope,
                        severity: :error,
                        scope_capture: :socket
                      }}
    end)
  end

  test "graph snapshot exposes derive sharing diagnostics", %{table: table} do
    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [{user_id, :user_issue}])
    :ets.insert(table, {{:loads, :shared_issue_count, user_id}, 0})

    source_id = {ScopedIssues, %{user_id: user_id}}

    shared_graph_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id],
       {__MODULE__, :shared_issue_count, 1}}

    visible_extra = user_id

    socket =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_id)
      |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)
      |> Live.derive(:visible_count, [:issue_count], fn %{issue_count: count} ->
        count + visible_extra
      end)

    snapshot = Live.graph_snapshot(socket)

    assert %{
             assign: :issue_count,
             node_id: {:derived, :issue_count},
             sharing: %{
               result: :shared,
               reason: :shareable,
               graph_node_id: ^shared_graph_id,
               graph_dep_node_ids: [^source_id],
               sharing_partition: %{user_id: ^user_id},
               dep_partitions: [{^source_id, %{user_id: ^user_id}}],
               fun: {__MODULE__, :shared_issue_count, 1}
             }
           } = Enum.find(snapshot.assigns, &(&1.assign == :issue_count))

    assert %{
             assign: :visible_count,
             node_id: {:derived, :visible_count},
             sharing: %{
               result: :local,
               reason: :captured_fun,
               dep_node_ids: [{:derived, :issue_count}]
             }
           } = Enum.find(snapshot.assigns, &(&1.assign == :visible_count))
  end

  test "concurrent connected derives share a chain of initial computes", %{table: table} do
    test_pid = self()
    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [{user_id, :user_issue}])
    :ets.insert(table, {{:loads, :shared_issue_stats, user_id}, 0})
    :ets.insert(table, {{:loads, :shared_issue_label, user_id}, 0})
    :ets.insert(table, {{:derive_test_pid, user_id}, test_pid})

    task_a =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
        |> Live.derive(:issue_label, [:issue_stats], &__MODULE__.shared_issue_label/1)
      end)

    assert_receive {:derived_compute_started, count_pid, ^user_id}

    task_b =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
        |> Live.derive(:issue_label, [:issue_stats], &__MODULE__.shared_issue_label/1)
      end)

    refute_receive {:derived_compute_started, _second_count_pid, ^user_id}, 50
    send(count_pid, :continue)

    assert_receive {:derived_label_started, label_pid, ^user_id}
    refute_receive {:derived_label_started, _second_label_pid, ^user_id}, 50
    send(label_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.issue_stats == %{user_id: user_id, count: 1}
    assert socket_a.assigns.issue_label == "1 issue"
    assert socket_b.assigns.issue_stats == %{user_id: user_id, count: 1}
    assert socket_b.assigns.issue_label == "1 issue"
    assert load_count(:shared_issue_stats, user_id) == 1
    assert load_count(:shared_issue_label, user_id) == 1
  end

  test "connected chained derived sharing does not leak values across source params", %{
    table: table
  } do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    put_scoped_user(table, user_a, [{user_a, :user_a_issue}])
    put_scoped_user(table, user_b, [{user_b, :user_b_issue}])
    :ets.insert(table, {{:loads, :shared_issue_stats, user_a}, 0})
    :ets.insert(table, {{:loads, :shared_issue_stats, user_b}, 0})
    :ets.insert(table, {{:loads, :shared_user_label, user_a}, 0})
    :ets.insert(table, {{:loads, :shared_user_label, user_b}, 0})

    socket_a =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
      |> Live.derive(:user_label, [:issue_stats], &__MODULE__.shared_user_label/1)

    socket_b =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)
      |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
      |> Live.derive(:user_label, [:issue_stats], &__MODULE__.shared_user_label/1)

    assert socket_a.assigns.user_label == "user #{user_a}: 1 issue"
    assert socket_b.assigns.user_label == "user #{user_b}: 1 issue"
    assert load_count(:shared_issue_stats, user_a) == 1
    assert load_count(:shared_issue_stats, user_b) == 1
    assert load_count(:shared_user_label, user_a) == 1
    assert load_count(:shared_user_label, user_b) == 1
  end

  test "concurrent connected derives share a multi-source initial compute", %{table: table} do
    test_pid = self()
    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [{user_id, :user_issue}])
    put_scoped_activity(table, user_id, [{user_id, :user_activity}])
    :ets.insert(table, {{:loads, :shared_dashboard_model, user_id}, 0})
    :ets.insert(table, {{:derive_test_pid, user_id}, test_pid})

    task_a =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.watch(:activity, ScopedActivity, user_id: user_id)
        |> Live.derive(
          :dashboard_model,
          [:issues, :activity],
          &__MODULE__.shared_dashboard_model/1
        )
      end)

    assert_receive {:dashboard_model_started, loader_pid, ^user_id}

    task_b =
      Task.async(fn ->
        connected_live_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.watch(:activity, ScopedActivity, user_id: user_id)
        |> Live.derive(
          :dashboard_model,
          [:issues, :activity],
          &__MODULE__.shared_dashboard_model/1
        )
      end)

    refute_receive {:dashboard_model_started, _second_loader_pid, ^user_id}, 50
    send(loader_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.dashboard_model == %{
             user_id: user_id,
             issues: [:user_issue],
             activity: [:user_activity]
           }

    assert socket_b.assigns.dashboard_model == socket_a.assigns.dashboard_model
    assert load_count(:shared_dashboard_model, user_id) == 1
  end

  test "same-param source modules are colocated for shared multi-source derives", %{table: table} do
    attach_telemetry([[:upkeep, :derive, :sharing]])

    project_id = System.unique_integer([:positive])
    :ets.insert(table, {{:issues, project_id}, [:project_issue]})
    :ets.insert(table, {{:activity, project_id}, [:project_activity]})
    :ets.insert(table, {{:loads, :issues, project_id}, 0})
    :ets.insert(table, {{:loads, :activity, project_id}, 0})
    :ets.insert(table, {{:loads, :shared_project_dashboard_model, project_id}, 0})

    source_ids = [
      {ProjectIssues, %{project_id: project_id}},
      {ProjectActivity, %{project_id: project_id}}
    ]

    graph_node_id =
      {:derived, UpkeepWeb.KanbanLive, :project_dashboard_model, source_ids,
       {__MODULE__, :shared_project_dashboard_model, 1}}

    socket =
      connected_live_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: project_id)
      |> Live.watch(:activity, ProjectActivity, project_id: project_id)
      |> Live.derive(
        :project_dashboard_model,
        [:issues, :activity],
        &__MODULE__.shared_project_dashboard_model/1
      )

    assert socket.assigns.project_dashboard_model == %{
             project_id: project_id,
             issues: [:project_issue],
             activity: [:project_activity]
           }

    assert load_count(:shared_project_dashboard_model, project_id) == 1

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :project_dashboard_model,
                      result: :shared,
                      reason: :shareable,
                      graph_node_id: ^graph_node_id,
                      graph_dep_node_ids: ^source_ids,
                      sharing_partition: %{project_id: ^project_id},
                      dep_partitions: [
                        {{ProjectIssues, %{project_id: ^project_id}}, %{project_id: ^project_id}},
                        {{ProjectActivity, %{project_id: ^project_id}},
                         %{project_id: ^project_id}}
                      ]
                    }}
  end

  test "cross-partition multi-source derives fall back locally without leaking values", %{
    table: table
  } do
    attach_telemetry([[:upkeep, :derive, :sharing]])

    project_id = System.unique_integer([:positive])
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])

    :ets.insert(table, {{:activity, project_id}, [:project_activity]})
    :ets.insert(table, {{:loads, :activity, project_id}, 0})
    put_scoped_user(table, user_a, [{user_a, :user_a_issue}])
    put_scoped_user(table, user_b, [{user_b, :user_b_issue}])
    :ets.insert(table, {{:loads, :shared_user_project_dashboard_model, user_a}, 0})
    :ets.insert(table, {{:loads, :shared_user_project_dashboard_model, user_b}, 0})

    socket_a =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.watch(:activity, ProjectActivity, project_id: project_id)
      |> Live.derive(
        :dashboard_model,
        [:issues, :activity],
        &__MODULE__.shared_user_project_dashboard_model/1
      )

    socket_b =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)
      |> Live.watch(:activity, ProjectActivity, project_id: project_id)
      |> Live.derive(
        :dashboard_model,
        [:issues, :activity],
        &__MODULE__.shared_user_project_dashboard_model/1
      )

    assert socket_a.assigns.dashboard_model == %{
             user_id: user_a,
             issues: [:user_a_issue],
             activity: [:project_activity]
           }

    assert socket_b.assigns.dashboard_model == %{
             user_id: user_b,
             issues: [:user_b_issue],
             activity: [:project_activity]
           }

    assert load_count(:shared_user_project_dashboard_model, user_a) == 1
    assert load_count(:shared_user_project_dashboard_model, user_b) == 1

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :dashboard_model,
                      result: :local,
                      reason: :cross_partition_dep,
                      dep_partitions: [
                        {{ScopedIssues, %{user_id: ^user_a}}, %{user_id: ^user_a}},
                        {{ProjectActivity, %{project_id: ^project_id}},
                         %{project_id: ^project_id}}
                      ]
                    }}

    assert_receive {:telemetry, [:upkeep, :derive, :sharing], %{count: 1},
                    %{
                      assign_name: :dashboard_model,
                      result: :local,
                      reason: :cross_partition_dep,
                      dep_partitions: [
                        {{ScopedIssues, %{user_id: ^user_b}}, %{user_id: ^user_b}},
                        {{ProjectActivity, %{project_id: ^project_id}},
                         %{project_id: ^project_id}}
                      ]
                    }}
  end

  test "connected multi-source derived sharing does not leak values across source params", %{
    table: table
  } do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    put_scoped_user(table, user_a, [{user_a, :user_a_issue}])
    put_scoped_user(table, user_b, [{user_b, :user_b_issue}])
    put_scoped_activity(table, user_a, [{user_a, :user_a_activity}])
    put_scoped_activity(table, user_b, [{user_b, :user_b_activity}])
    :ets.insert(table, {{:loads, :shared_dashboard_model, user_a}, 0})
    :ets.insert(table, {{:loads, :shared_dashboard_model, user_b}, 0})

    socket_a =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.watch(:activity, ScopedActivity, user_id: user_a)
      |> Live.derive(:dashboard_model, [:issues, :activity], &__MODULE__.shared_dashboard_model/1)

    socket_b =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)
      |> Live.watch(:activity, ScopedActivity, user_id: user_b)
      |> Live.derive(:dashboard_model, [:issues, :activity], &__MODULE__.shared_dashboard_model/1)

    assert socket_a.assigns.dashboard_model == %{
             user_id: user_a,
             issues: [:user_a_issue],
             activity: [:user_a_activity]
           }

    assert socket_b.assigns.dashboard_model == %{
             user_id: user_b,
             issues: [:user_b_issue],
             activity: [:user_b_activity]
           }

    assert load_count(:shared_dashboard_model, user_a) == 1
    assert load_count(:shared_dashboard_model, user_b) == 1
  end

  test "graph-pushed source updates reuse shared derived graph values", %{table: table} do
    attach_telemetry([[:upkeep, :graph, :dispatch, :stop]])

    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [{user_id, :before}])
    :ets.insert(table, {{:loads, :shared_issue_count, user_id}, 0})

    source_id = {ScopedIssues, %{user_id: user_id}}

    socket =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_id)
      |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    assert socket.assigns.issue_count == 1
    assert load_count(:shared_issue_count, user_id) == 1

    :ets.insert(table, {{:scoped_issues, user_id}, [{user_id, :before}, {user_id, :after}]})

    assert :ok =
             %Issue{issue_id: user_id}
             |> Upkeep.Change.updated()
             |> Upkeep.notify()

    graph_node_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id],
       {__MODULE__, :shared_issue_count, 1}}

    assert_receive {:dag_values, pairs}
    assert {source_id, [{user_id, :before}, {user_id, :after}]} in pairs
    assert {graph_node_id, 2} in pairs

    assert_receive {:telemetry, [:upkeep, :graph, :dispatch, :stop], _measurements,
                    %{
                      node_partitions: [
                        {^source_id, %{user_id: ^user_id}},
                        {^graph_node_id, %{user_id: ^user_id}}
                      ]
                    }}

    socket = Live.apply_dag_values(socket, pairs)

    assert socket.assigns.issues == [{user_id, :before}, {user_id, :after}]
    assert socket.assigns.issue_count == 2
    assert load_count(:shared_issue_count, user_id) == 2
  end

  test "local derives recompute after graph-pushed shared derived values", %{table: table} do
    user_id = System.unique_integer([:positive])
    put_scoped_user(table, user_id, [{user_id, :before}])
    :ets.insert(table, {{:loads, :shared_issue_count, user_id}, 0})

    extra = user_id
    source_id = {ScopedIssues, %{user_id: user_id}}

    socket =
      connected_live_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_id)
      |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)
      |> Live.derive(:visible_count, [:issue_count], fn %{issue_count: count} -> count + extra end)

    assert socket.assigns.visible_count == user_id + 1

    :ets.insert(table, {{:scoped_issues, user_id}, [{user_id, :before}, {user_id, :after}]})

    assert :ok =
             %Issue{issue_id: user_id}
             |> Upkeep.Change.updated()
             |> Upkeep.notify()

    graph_node_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id],
       {__MODULE__, :shared_issue_count, 1}}

    assert_receive {:dag_values, pairs}
    assert {graph_node_id, 2} in pairs

    socket = Live.apply_dag_values(socket, pairs)

    assert socket.assigns.issue_count == 2
    assert socket.assigns.visible_count == user_id + 2
    assert load_count(:shared_issue_count, user_id) == 2
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

  test "disconnected LiveView-shaped watches load without joining source interest" do
    socket =
      disconnected_live_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue_a]
    assert load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 0

    _socket = Live.unwatch(socket, :issues)

    assert member_count(ProjectIssues, project_id: 1) == 0
  end

  test "connected LiveView-shaped watches join source interest" do
    socket =
      connected_live_socket()
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
    refute_receive {:dag_values, [{_, _}]}

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

  test "function component identity can scope sources without an Upkeep frame", %{table: table} do
    component_id = {__MODULE__.IssueComponents, :issue_card, 1}

    socket =
      new_socket()
      |> Live.component(component_id, [], fn %{} -> %{issue_id: 1} end)
      |> Live.watch(:issue_card_comments, IssueComments, [issue_id: 1], under: component_id)
      |> Live.derive(:issue_card_comment_count, [:issue_card_comments], fn %{
                                                                             issue_card_comments:
                                                                               comments
                                                                           } ->
        length(comments)
      end)

    assert socket.assigns.issue_card_comments == [:comment_a]
    assert socket.assigns.issue_card_comment_count == 1
    assert member_count(IssueComments, [issue_id: 1], component_id) == 1

    snapshot = Live.graph_snapshot(socket)
    source_id = {:scoped, component_id, {IssueComments, %{issue_id: 1}}}

    assert %{id: {:component, ^component_id}, kind: :component} =
             Enum.find(snapshot.dag.nodes, &(&1.id == {:component, component_id}))

    assert %{from: {:component, ^component_id}, to: {:source, ^source_id}} =
             Enum.find(snapshot.dag.edges, &(&1.to == {:source, source_id}))

    socket = Live.remove_component(socket, component_id)

    assert member_count(IssueComments, issue_id: 1) == 0

    :ets.insert(table, {{:comments, 1}, [:comment_a, :comment_c]})

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_card_comments == [:comment_a]
    assert socket.assigns.issue_card_comment_count == 1
    assert load_count(:comments, 1) == 1
  end

  test "function component input changes remove stale scoped source reads", %{table: table} do
    component_id = {__MODULE__.IssueComponents, :issue_card, 1}
    :ets.insert(table, {{:issues, 1}, [1]})

    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.component(component_id, [:issues], fn %{issues: [issue_id | _]} ->
        %{issue_id: issue_id}
      end)
      |> Live.derive(:issue_label, [:issue_id], fn %{issue_id: issue_id} ->
        "issue #{issue_id}"
      end)
      |> Live.watch(:issue_card_comments, IssueComments, [issue_id: 1], under: component_id)

    assert socket.assigns.issue_id == 1
    assert socket.assigns.issue_label == "issue 1"
    assert socket.assigns.issue_card_comments == [:comment_a]
    assert member_count(IssueComments, [issue_id: 1], component_id) == 1

    :ets.insert(table, {{:issues, 1}, [1, :unchanged_component_value]})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_id == 1
    assert socket.assigns.issue_label == "issue 1"
    assert member_count(IssueComments, [issue_id: 1], component_id) == 1

    :ets.insert(table, {{:issues, 1}, [2]})

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_id == 2
    assert socket.assigns.issue_label == "issue 2"
    assert member_count(IssueComments, issue_id: 1) == 0

    :ets.insert(table, {{:comments, 1}, [:comment_a, :comment_c]})

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_card_comments == [:comment_a]
    assert load_count(:comments, 1) == 1

    socket =
      Live.watch(socket, :issue_card_comments, IssueComments, [issue_id: 2], under: component_id)

    assert socket.assigns.issue_card_comments == [:comment_b]
    assert member_count(IssueComments, [issue_id: 2], component_id) == 1
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

    :ets.insert(table, {{:comments, 1}, [:comment_a, :comment_c]})

    change = inserted_comment(1, 1)
    source_id = {IssueComments, %{issue_id: 1}}
    assert :ok = Upkeep.notify(change)
    assert :ok = Upkeep.Coordinator.Graph.drain()
    assert_receive {:dag_values, [{^source_id, [:comment_a, :comment_c]}]}

    socket = Live.apply_dag_value(socket, source_id, [:comment_a, :comment_c])

    assert socket.assigns.comments == [:comment_a, :comment_c]
    assert load_count(:comments, 1) == 3

    _socket = Live.unwatch(socket, :comments)

    assert member_count(IssueComments, issue_id: 1) == 0
  end

  test "graph snapshot exposes assigns, watches, and pending refreshes" do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], fn %{issues: issues} -> length(issues) end)
      |> Live.component(:issue_detail, [:issues], fn %{issues: issues} ->
        %{first_issue: List.first(issues)}
      end)
      |> Live.watch(:comments, IssueComments, [issue_id: 1], under: :issue_detail)
      |> Live.derive(:comment_count, [:comments], fn %{comments: comments} -> length(comments) end)
      |> Live.queue_matching(inserted_comment(1, 1))

    snapshot = Live.graph_snapshot(socket)

    issue_source_id = {ProjectIssues, %{project_id: 1}}
    comments_source_id = {:scoped, :issue_detail, {IssueComments, %{issue_id: 1}}}

    assert %{assign: :issues, node_id: {:source, ^issue_source_id}} =
             Enum.find(snapshot.assigns, &(&1.assign == :issues))

    assert %{assign: :comment_count, node_id: {:derived, :comment_count}} =
             Enum.find(snapshot.assigns, &(&1.assign == :comment_count))

    assert %{
             source_id: ^comments_source_id,
             component: :issue_detail,
             assign_names: [:comments],
             sharing_partition: %{issue_id: 1}
           } =
             Enum.find(snapshot.watches, &(&1.source_id == comments_source_id))

    assert snapshot.pending_refreshes == [comments_source_id]
    assert {:component, :issue_detail} in snapshot.dag.topological_order
    assert {:source, comments_source_id} in snapshot.dag.topological_order

    assert %{from: {:component, :issue_detail}, to: {:source, ^comments_source_id}} =
             Enum.find(snapshot.dag.edges, &(&1.to == {:source, comments_source_id}))
  end

  test "emits telemetry for watch, shared initial load, queue, reload, recompute, assign, and unwatch",
       %{
         table: table
       } do
    attach_telemetry([
      [:upkeep, :source, :watch],
      [:upkeep, :source, :queue],
      [:upkeep, :source, :reload, :start],
      [:upkeep, :source, :reload, :stop],
      [:upkeep, :source, :unwatch],
      [:upkeep, :dag, :recompute, :start],
      [:upkeep, :dag, :recompute, :stop],
      [:upkeep, :live, :assign],
      [:upkeep, :graph, :initial_load, :miss]
    ])

    source_id = {ProjectIssues, %{project_id: 1}}

    socket =
      connected_live_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], fn %{issues: issues} -> length(issues) end)

    assert_receive {:telemetry, [:upkeep, :graph, :initial_load, :miss], %{count: 1},
                    %{
                      node_id: ^source_id,
                      source: ProjectIssues,
                      params: %{project_id: 1},
                      sharing_partition: %{project_id: 1}
                    }}

    assert_receive {:telemetry, [:upkeep, :source, :watch], %{count: 1},
                    %{
                      source_id: ^source_id,
                      assign_name: :issues,
                      kind: :new,
                      sharing_partition: %{project_id: 1}
                    }}

    assert_receive {:telemetry, [:upkeep, :live, :assign], %{count: 1},
                    %{assign: :issues, node_id: {:source, ^source_id}, kind: :source}}

    :ets.insert(table, {{:issues, 1}, [:issue_a, :issue_b]})
    change = updated_issue(1, 1)

    socket = Live.queue_matching(socket, change)

    assert_receive {:telemetry, [:upkeep, :source, :queue], %{count: 1},
                    %{source_id: ^source_id, event: ^change}}

    socket = Live.flush_refreshes(socket)

    assert_receive {:telemetry, [:upkeep, :source, :reload, :start], _measurements,
                    %{
                      source_id: ^source_id,
                      reason: :refresh,
                      sharing_partition: %{project_id: 1}
                    }}

    assert_receive {:telemetry, [:upkeep, :source, :reload, :stop], measurements,
                    %{
                      source_id: ^source_id,
                      reason: :refresh,
                      sharing_partition: %{project_id: 1}
                    }}

    assert is_integer(measurements.duration)

    assert_receive {:telemetry, [:upkeep, :dag, :recompute, :stop], _measurements,
                    %{
                      changed_source_nodes: [{:source, ^source_id}],
                      changed_derived_nodes: [{:derived, :issue_count}],
                      recomputed_nodes: [{:derived, :issue_count}],
                      changed_count: 1,
                      recomputed_count: 1
                    }}

    assert_receive {:telemetry, [:upkeep, :live, :assign], %{count: 1},
                    %{assign: :issue_count, node_id: {:derived, :issue_count}, kind: :derived}}

    _socket = Live.unwatch(socket, :issues)

    assert_receive {:telemetry, [:upkeep, :source, :unwatch], %{count: 1},
                    %{source_id: ^source_id, kind: :remove}}
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}

  defp disconnected_live_socket do
    %Phoenix.LiveView.Socket{
      endpoint: UpkeepWeb.Endpoint,
      view: UpkeepWeb.KanbanLive,
      assigns: %{__changed__: %{}}
    }
  end

  defp connected_live_socket do
    %Phoenix.LiveView.Socket{
      endpoint: UpkeepWeb.Endpoint,
      view: UpkeepWeb.KanbanLive,
      transport_pid: self(),
      assigns: %{__changed__: %{}}
    }
  end

  defp connected_live_socket(current_scope) do
    connected_live_socket()
    |> Phoenix.Component.assign(:current_scope, current_scope)
  end

  def table_value(key) do
    [{^key, value}] = :ets.lookup(__MODULE__, key)
    value
  end

  def bump_load(key) do
    :ets.update_counter(__MODULE__, key, 1)
  end

  def shared_issue_count(%{issues: [{user_id, _name} | _] = issues}) do
    bump_load({:loads, :shared_issue_count, user_id})

    case :ets.lookup(__MODULE__, {:derive_test_pid, user_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:derived_compute_started, self(), user_id})

        receive do
          :continue -> :ok
        after
          1_000 -> raise "blocking derived compute was not released"
        end

      [] ->
        :ok
    end

    length(issues)
  end

  def shared_local_issue_count(%{local_issues: issues}), do: length(issues)

  def shared_issue_names(%{issues: [{user_id, _name} | _] = issues}) do
    bump_load({:loads, :shared_issue_names, user_id})
    Enum.map(issues, fn {_user_id, name} -> name end)
  end

  def shared_issue_stats(%{issues: [{user_id, _name} | _] = issues}) do
    bump_load({:loads, :shared_issue_stats, user_id})

    case :ets.lookup(__MODULE__, {:derive_test_pid, user_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:derived_compute_started, self(), user_id})

        receive do
          :continue -> :ok
        after
          1_000 -> raise "blocking derived compute was not released"
        end

      [] ->
        :ok
    end

    %{user_id: user_id, count: length(issues)}
  end

  def shared_issue_label(%{issue_stats: %{user_id: user_id, count: count}}) do
    bump_load({:loads, :shared_issue_label, user_id})

    case :ets.lookup(__MODULE__, {:derive_test_pid, user_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:derived_label_started, self(), user_id})

        receive do
          :continue -> :ok
        after
          1_000 -> raise "blocking derived label compute was not released"
        end

      [] ->
        :ok
    end

    "#{count} issue"
  end

  def shared_user_label(%{issue_stats: %{user_id: user_id, count: count}}) do
    bump_load({:loads, :shared_user_label, user_id})
    "user #{user_id}: #{count} issue"
  end

  def shared_dashboard_model(%{issues: [{user_id, _issue} | _] = issues, activity: activity}) do
    bump_load({:loads, :shared_dashboard_model, user_id})

    case :ets.lookup(__MODULE__, {:derive_test_pid, user_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:dashboard_model_started, self(), user_id})

        receive do
          :continue -> :ok
        after
          1_000 -> raise "blocking dashboard model compute was not released"
        end

      [] ->
        :ok
    end

    %{
      user_id: user_id,
      issues: Enum.map(issues, fn {_user_id, issue} -> issue end),
      activity: Enum.map(activity, fn {_user_id, item} -> item end)
    }
  end

  def shared_project_dashboard_model(%{issues: issues, activity: activity}) do
    project_id =
      issues
      |> hd()
      |> case do
        {id, _issue} -> id
        _issue -> :unknown
      end

    project_id =
      case :ets.match(__MODULE__, {{:issues, :"$1"}, issues}) do
        [[id] | _] -> id
        [] -> project_id
      end

    bump_load({:loads, :shared_project_dashboard_model, project_id})

    %{
      project_id: project_id,
      issues: issues,
      activity: activity
    }
  end

  def shared_user_project_dashboard_model(%{
        issues: [{user_id, _issue} | _] = issues,
        activity: activity
      }) do
    bump_load({:loads, :shared_user_project_dashboard_model, user_id})

    %{
      user_id: user_id,
      issues: Enum.map(issues, fn {_user_id, issue} -> issue end),
      activity: activity
    }
  end

  defp load_count(source) do
    table_value({:loads, source, 1})
  end

  defp load_count(source, id) do
    table_value({:loads, source, id})
  end

  defp put_scoped_user(table, user_id, issues) do
    :ets.insert(table, {{:scoped_issues, user_id}, issues})
    :ets.insert(table, {{:loads, :scoped_issues, user_id}, 0})
  end

  defp put_scoped_activity(table, user_id, activity) do
    :ets.insert(table, {{:scoped_activity, user_id}, activity})
    :ets.insert(table, {{:loads, :scoped_activity, user_id}, 0})
  end

  defp updated_issue(project_id, issue_id) do
    %Issue{project_id: project_id, issue_id: issue_id}
    |> Upkeep.Change.updated()
  end

  defp inserted_comment(project_id, issue_id) do
    %Comment{project_id: project_id, issue_id: issue_id}
    |> Upkeep.Change.inserted()
  end

  defp member_count(source, params, component \\ :issue_detail) do
    params = Map.new(params)

    source_ids = [
      Upkeep.Live.Ids.scoped_source_id(source, params, nil),
      Upkeep.Live.Ids.scoped_source_id(source, params, component)
    ]

    subscribed? =
      Enum.any?(source_ids, fn source_id ->
        Upkeep.Coordinator.Graph.subscribed?(source_id, self())
      end)

    if subscribed?, do: 1, else: 0
  end

  defp attach_telemetry(events) do
    test_pid = self()
    handler_id = {__MODULE__, test_pid, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp with_captured_scope_policy(policy, fun) do
    previous = Application.get_env(:upkeep, :captured_scope_policy, :__missing__)
    Application.put_env(:upkeep, :captured_scope_policy, policy)

    try do
      fun.()
    after
      case previous do
        :__missing__ -> Application.delete_env(:upkeep, :captured_scope_policy)
        value -> Application.put_env(:upkeep, :captured_scope_policy, value)
      end
    end
  end
end
