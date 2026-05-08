defmodule Upkeep.LiveRefreshTest do
  use ExUnit.Case, async: false

  alias Upkeep.Live

  import Upkeep.TestSupport, only: [attach_telemetry: 1]

  import Upkeep.TestSupport.LiveSocket,
    only: [
      connected_socket: 0,
      disconnected_socket: 0,
      scoped_connected_socket: 1,
      socket: 0
    ]

  alias Upkeep.TestSupport.{Config, DagMessages, TelemetryMessages}
  alias Upkeep.TestSupport.LiveRefreshFixture, as: Fixture

  defmodule Issue do
    defstruct [:project_id, :issue_id]
  end

  defmodule Comment do
    defstruct [:project_id, :issue_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source
    alias Upkeep.TestSupport.LiveRefreshFixture, as: Fixture

    def load(s) do
      Fixture.load_source_value(:issues, s.project_id)
    end

    invalidated_by(Issue, :updated, on: :project_id)
  end

  defmodule ProjectActivity do
    use Upkeep.Source
    alias Upkeep.TestSupport.LiveRefreshFixture, as: Fixture

    def load(s) do
      Fixture.load_source_value(:activity, s.project_id)
    end

    invalidated_by(Issue, :updated, on: :project_id)
    invalidated_by(Comment, :inserted, on: :project_id)
  end

  defmodule FailingIssues do
    use Upkeep.Source
    alias Upkeep.TestSupport.LiveRefreshFixture, as: Fixture

    def load(s) do
      case Fixture.load_source_value(:failing, s.project_id) do
        :raise -> raise "source failed"
        value -> value
      end
    end

    invalidated_by(Issue, :updated, on: :project_id)
  end

  defmodule IssueComments do
    use Upkeep.Source
    alias Upkeep.TestSupport.LiveRefreshFixture, as: Fixture

    def load(s) do
      Fixture.load_source_value(:comments, s.issue_id)
    end

    invalidated_by(Comment, :inserted, on: :issue_id)
  end

  defmodule ScopedIssues do
    use Upkeep.Source
    alias Upkeep.TestSupport.LiveRefreshFixture, as: Fixture

    def load(s) do
      Fixture.load_source_value(:scoped_issues, s.user_id)
    end

    invalidated_by(Issue, :updated, on: :issue_id, as: :user_id)
  end

  defmodule ScopedActivity do
    use Upkeep.Source
    alias Upkeep.TestSupport.LiveRefreshFixture, as: Fixture

    def load(s) do
      Fixture.load_source_value(:scoped_activity, s.user_id)
    end

    invalidated_by(Comment, :inserted, on: :issue_id, as: :user_id)
  end

  defmodule BlockingScopedIssues do
    use Upkeep.Source
    alias Upkeep.TestSupport.LiveRefreshFixture

    def load(s) do
      send(s.test_pid, {:blocking_load_started, self()})

      receive do
        :continue -> :ok
      after
        1_000 -> raise "blocking source was not released"
      end

      LiveRefreshFixture.load_source_value(:scoped_issues, s.user_id)
    end

    invalidated_by(Issue, :updated, on: :issue_id, as: :user_id)
  end

  setup do
    Upkeep.Test.reset_graph()

    Fixture.setup!()
    on_exit(&Fixture.reset!/0)
    :ok
  end

  test "many changes for one source reload once per flush" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert Fixture.load_count(:issues) == 1

    Fixture.set_source_value(:issues, 1, [:issue_b])

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.queue_matching(updated_issue(1, 2))
      |> Live.queue_matching(updated_issue(1, 3))
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_b]
    assert Fixture.load_count(:issues) == 2
  end

  test "different affected sources each reload once" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:activity, ProjectActivity, project_id: 1)

    assert Fixture.load_count(:issues) == 1
    assert Fixture.load_count(:activity) == 1

    Fixture.set_source_value(:issues, 1, [:issue_b])
    Fixture.set_source_value(:activity, 1, [:activity_b])

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_b]
    assert socket.assigns.activity == [:activity_b]
    assert Fixture.load_count(:issues) == 2
    assert Fixture.load_count(:activity) == 2
  end

  test "failed source load raises" do
    socket =
      socket()
      |> Live.watch(:issues, FailingIssues, project_id: 1)

    assert Fixture.load_count(:failing) == 1

    Fixture.set_source_value(:failing, 1, :raise)

    assert_raise RuntimeError, "source failed", fn ->
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()
    end

    assert Fixture.load_count(:failing) == 2
  end

  test "queues are per socket process state" do
    socket_a = Live.watch(socket(), :issues, ProjectIssues, project_id: 1)
    socket_b = Live.watch(socket(), :issues, ProjectIssues, project_id: 1)

    Fixture.set_source_value(:issues, 1, [:issue_b])

    socket_a =
      socket_a
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket_a.assigns.issues == [:issue_b]
    assert socket_b.assigns.issues == [:issue_a]
  end

  test "concurrent connected watches share the in-flight initial load for the same source identity" do
    attach_telemetry([[:upkeep, :source, :initial_load, :coalesced]])

    test_pid = self()
    user_id = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_id, [:user_issue])
    source_id = {BlockingScopedIssues, %{test_pid: test_pid, user_id: user_id}}

    task_a =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    assert_receive {:blocking_load_started, loader_pid}

    task_b =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    TelemetryMessages.assert_counted([:upkeep, :source, :initial_load, :coalesced],
      node_id: source_id
    )

    send(loader_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.issues == [:user_issue]
    assert socket_b.assigns.issues == [:user_issue]
    assert Fixture.load_count(:scoped_issues, user_id) == 1
  end

  test "connected watches do not share initial values across different source params" do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_a, [:user_a_issue])
    Fixture.seed_scoped_issues(user_b, [:user_b_issue])

    socket_a =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)

    socket_b =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)

    assert socket_a.assigns.issues == [:user_a_issue]
    assert socket_b.assigns.issues == [:user_b_issue]
    assert Fixture.load_count(:scoped_issues, user_a) == 1
    assert Fixture.load_count(:scoped_issues, user_b) == 1
  end

  test "connected source sharing emits initial-load miss and coalesced telemetry" do
    attach_telemetry([
      [:upkeep, :source, :initial_load, :miss],
      [:upkeep, :source, :initial_load, :coalesced]
    ])

    test_pid = self()
    user_id = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_id, [:user_issue])
    source_id = {BlockingScopedIssues, %{test_pid: test_pid, user_id: user_id}}

    task_a =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    assert_receive {:blocking_load_started, loader_pid}

    task_b =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, BlockingScopedIssues, user_id: user_id, test_pid: test_pid)
      end)

    TelemetryMessages.assert_counted([:upkeep, :source, :initial_load, :coalesced],
      node_id: source_id
    )

    send(loader_pid, :continue)

    Task.await(task_a)
    Task.await(task_b)

    TelemetryMessages.assert_counted([:upkeep, :source, :initial_load, :miss],
      node_id: {:source, source_id},
      source: BlockingScopedIssues,
      params: %{user_id: user_id},
      sharing_partition: %{user_id: user_id}
    )
  end

  test "concurrent connected derives share the in-flight initial compute for the same source identity" do
    attach_telemetry([
      [:upkeep, :graph, :derived_initial, :miss],
      [:upkeep, :graph, :derived_initial, :hit]
    ])

    test_pid = self()
    user_id = System.unique_integer([:positive])

    Fixture.seed_scoped_issues(user_id, [{user_id, :user_issue}])
    Fixture.seed_load_counter(:shared_issue_count, user_id)
    Fixture.block_derives_for(user_id, test_pid)

    source_id = {ScopedIssues, %{user_id: user_id}}

    graph_node_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id],
       {__MODULE__, :shared_issue_count, 1}}

    task_a =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)
      end)

    assert_receive {:derived_compute_started, loader_pid, ^user_id}

    task_b =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)
      end)

    TelemetryMessages.assert_counted(
      [:upkeep, :graph, :derived_initial, :hit],
      %{
        assign_name: :issue_count,
        node_id: graph_node_id,
        view: UpkeepWeb.KanbanLive,
        dep_node_ids: [source_id],
        sharing_partition: %{user_id: user_id},
        dep_partitions: [{source_id, %{user_id: user_id}}],
        fun: {__MODULE__, :shared_issue_count, 1}
      }
    )

    send(loader_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.issue_count == 1
    assert socket_b.assigns.issue_count == 1
    assert Fixture.load_count(:shared_issue_count, user_id) == 1

    TelemetryMessages.assert_counted(
      [:upkeep, :graph, :derived_initial, :miss],
      %{
        assign_name: :issue_count,
        node_id: graph_node_id,
        view: UpkeepWeb.KanbanLive,
        dep_node_ids: [source_id],
        sharing_partition: %{user_id: user_id},
        dep_partitions: [{source_id, %{user_id: user_id}}],
        fun: {__MODULE__, :shared_issue_count, 1}
      }
    )
  end

  test "connected derived sharing does not leak values across source params" do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_a, [{user_a, :user_a_issue}])
    Fixture.seed_scoped_issues(user_b, [{user_b, :user_b_issue}])
    Fixture.seed_load_counter(:shared_issue_names, user_a)
    Fixture.seed_load_counter(:shared_issue_names, user_b)

    socket_a =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.derive(:issue_names, [:issues], &__MODULE__.shared_issue_names/1)

    socket_b =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)
      |> Live.derive(:issue_names, [:issues], &__MODULE__.shared_issue_names/1)

    assert socket_a.assigns.issue_names == [:user_a_issue]
    assert socket_b.assigns.issue_names == [:user_b_issue]
    assert Fixture.load_count(:shared_issue_names, user_a) == 1
    assert Fixture.load_count(:shared_issue_names, user_b) == 1
  end

  test "derive sharing emits diagnostics for shared and local decisions" do
    attach_telemetry([[:upkeep, :derive, :sharing]])

    user_id = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_id, [{user_id, :user_issue}])
    Fixture.seed_load_counter(:shared_issue_count, user_id)

    shared_source_id = {ScopedIssues, %{user_id: user_id}}

    shared_graph_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [shared_source_id],
       {__MODULE__, :shared_issue_count, 1}}

    connected_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    TelemetryMessages.assert_counted(
      [:upkeep, :derive, :sharing],
      %{
        assign_name: :issue_count,
        result: :shared,
        reason: :shareable,
        graph_node_id: shared_graph_id,
        dep_node_ids: [{:source, shared_source_id}],
        graph_dep_node_ids: [shared_source_id],
        sharing_partition: %{user_id: user_id},
        dep_partitions: [{shared_source_id, %{user_id: user_id}}],
        fun: {__MODULE__, :shared_issue_count, 1}
      }
    )

    disconnected_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    TelemetryMessages.assert_counted([:upkeep, :derive, :sharing], %{
      assign_name: :issue_count,
      result: :local,
      reason: :disconnected_socket
    })

    issue_count_fn = fn %{issues: issues} -> length(issues) + user_id end

    connected_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:issue_count, [:issues], issue_count_fn)

    TelemetryMessages.assert_counted([:upkeep, :derive, :sharing], %{
      assign_name: :issue_count,
      result: :local,
      reason: :captured_fun
    })

    connected_socket()
    |> Live.component(:issue_detail, [], fn %{} -> %{issue_id: user_id} end)
    |> Live.watch(:issues, ScopedIssues, [user_id: user_id], under: :issue_detail)
    |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    TelemetryMessages.assert_counted([:upkeep, :derive, :sharing], %{
      assign_name: :issue_count,
      result: :local,
      reason: :component_scoped_dep
    })

    local_marker = :local

    connected_socket()
    |> Live.watch(:issues, ScopedIssues, user_id: user_id)
    |> Live.derive(:local_issues, [:issues], fn %{issues: issues} ->
      {local_marker, issues} |> elem(1)
    end)
    |> Live.derive(:issue_count, [:local_issues], &__MODULE__.shared_local_issue_count/1)

    TelemetryMessages.assert_counted([:upkeep, :derive, :sharing], %{
      assign_name: :local_issues,
      result: :local,
      reason: :local_fun
    })

    TelemetryMessages.assert_counted([:upkeep, :derive, :sharing], %{
      assign_name: :issue_count,
      result: :local,
      reason: :local_only_dep
    })
  end

  test "private derives receive current_scope implicitly and recompute when it changes" do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])
    Fixture.seed_source(:issues, user_a, [:user_a_issue])

    socket =
      scoped_connected_socket(%{user_id: user_a})
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

  test "capturing socket scope raises under strict policy before sharing data" do
    Config.with_upkeep(:captured_scope_policy, :raise, fn ->
      user_id = System.unique_integer([:positive])
      Fixture.seed_source(:issues, user_id, [:user_issue])
      Fixture.seed_load_counter(:captured_scope_label, user_id)

      socket =
        scoped_connected_socket(%{user_id: user_id})
        |> Live.watch(:issues, ProjectIssues, project_id: user_id)

      assert_raise Upkeep.Runtime.ImplicitScopeError, ~r/captures :socket/, fn ->
        Live.derive(socket, :captured_scope_label, [:issues], fn %{issues: issues} ->
          Fixture.bump_load({:loads, :captured_scope_label, user_id})
          {socket.assigns.current_scope.user_id, issues}
        end)
      end

      assert Fixture.load_count(:captured_scope_label, user_id) == 0
    end)
  end

  test "capturing socket scope stays local with error telemetry in production policy" do
    Config.with_upkeep(:captured_scope_policy, :telemetry, fn ->
      attach_telemetry([[:upkeep, :derive, :sharing]])

      user_a = System.unique_integer([:positive])
      user_b = System.unique_integer([:positive])
      Fixture.seed_source(:issues, user_a, [:shared_project_issue])
      Fixture.seed_load_counter(:captured_scope_label, user_a)
      Fixture.seed_load_counter(:captured_scope_label, user_b)

      base_a =
        scoped_connected_socket(%{user_id: user_a})
        |> Live.watch(:issues, ProjectIssues, project_id: user_a)

      base_b =
        scoped_connected_socket(%{user_id: user_b})
        |> Live.watch(:issues, ProjectIssues, project_id: user_a)

      socket_a =
        Live.derive(base_a, :captured_scope_label, [:issues], fn %{issues: issues} ->
          current_user_id = base_a.assigns.current_scope.user_id
          Fixture.bump_load({:loads, :captured_scope_label, current_user_id})
          {current_user_id, issues}
        end)

      socket_b =
        Live.derive(base_b, :captured_scope_label, [:issues], fn %{issues: issues} ->
          current_user_id = base_b.assigns.current_scope.user_id
          Fixture.bump_load({:loads, :captured_scope_label, current_user_id})
          {current_user_id, issues}
        end)

      assert socket_a.assigns.captured_scope_label == {user_a, [:shared_project_issue]}
      assert socket_b.assigns.captured_scope_label == {user_b, [:shared_project_issue]}
      assert Fixture.load_count(:captured_scope_label, user_a) == 1
      assert Fixture.load_count(:captured_scope_label, user_b) == 1

      TelemetryMessages.assert_counted([:upkeep, :derive, :sharing], %{
        assign_name: :captured_scope_label,
        result: :local,
        reason: :captured_scope,
        severity: :error,
        scope_capture: :socket
      })

      TelemetryMessages.assert_counted([:upkeep, :derive, :sharing], %{
        assign_name: :captured_scope_label,
        result: :local,
        reason: :captured_scope,
        severity: :error,
        scope_capture: :socket
      })
    end)
  end

  test "graph snapshot exposes derive sharing diagnostics" do
    user_id = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_id, [{user_id, :user_issue}])
    Fixture.seed_load_counter(:shared_issue_count, user_id)

    source_id = {ScopedIssues, %{user_id: user_id}}

    shared_graph_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id],
       {__MODULE__, :shared_issue_count, 1}}

    visible_extra = user_id

    socket =
      connected_socket()
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

  test "concurrent connected derives share a chain of initial computes" do
    attach_telemetry([[:upkeep, :graph, :derived_initial, :hit]])

    test_pid = self()
    user_id = System.unique_integer([:positive])

    Fixture.seed_scoped_issues(user_id, [{user_id, :user_issue}])
    Fixture.seed_load_counters([:shared_issue_stats, :shared_issue_label], user_id)
    Fixture.block_derives_for(user_id, test_pid)

    task_a =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
        |> Live.derive(:issue_label, [:issue_stats], &__MODULE__.shared_issue_label/1)
      end)

    assert_receive {:derived_compute_started, count_pid, ^user_id}

    task_b =
      Task.async(fn ->
        connected_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
        |> Live.derive(:issue_label, [:issue_stats], &__MODULE__.shared_issue_label/1)
      end)

    TelemetryMessages.assert_counted([:upkeep, :graph, :derived_initial, :hit], %{
      assign_name: :issue_stats
    })

    send(count_pid, :continue)

    assert_receive {:derived_label_started, label_pid, ^user_id}

    TelemetryMessages.assert_counted([:upkeep, :graph, :derived_initial, :hit], %{
      assign_name: :issue_label
    })

    send(label_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.issue_stats == %{user_id: user_id, count: 1}
    assert socket_a.assigns.issue_label == "1 issue"
    assert socket_b.assigns.issue_stats == %{user_id: user_id, count: 1}
    assert socket_b.assigns.issue_label == "1 issue"
    assert Fixture.load_count(:shared_issue_stats, user_id) == 1
    assert Fixture.load_count(:shared_issue_label, user_id) == 1
  end

  test "connected chained derived sharing does not leak values across source params" do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])

    Fixture.seed_scoped_issues(user_a, [{user_a, :user_a_issue}])
    Fixture.seed_scoped_issues(user_b, [{user_b, :user_b_issue}])
    Fixture.seed_load_counters([:shared_issue_stats, :shared_user_label], user_a)
    Fixture.seed_load_counters([:shared_issue_stats, :shared_user_label], user_b)

    socket_a =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
      |> Live.derive(:user_label, [:issue_stats], &__MODULE__.shared_user_label/1)

    socket_b =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_b)
      |> Live.derive(:issue_stats, [:issues], &__MODULE__.shared_issue_stats/1)
      |> Live.derive(:user_label, [:issue_stats], &__MODULE__.shared_user_label/1)

    assert socket_a.assigns.user_label == "user #{user_a}: 1 issue"
    assert socket_b.assigns.user_label == "user #{user_b}: 1 issue"
    assert Fixture.load_count(:shared_issue_stats, user_a) == 1
    assert Fixture.load_count(:shared_issue_stats, user_b) == 1
    assert Fixture.load_count(:shared_user_label, user_a) == 1
    assert Fixture.load_count(:shared_user_label, user_b) == 1
  end

  test "concurrent connected derives share a multi-source initial compute" do
    attach_telemetry([[:upkeep, :graph, :derived_initial, :hit]])

    test_pid = self()
    user_id = System.unique_integer([:positive])

    Fixture.seed_scoped_issues(user_id, [{user_id, :user_issue}])
    Fixture.seed_scoped_activity(user_id, [{user_id, :user_activity}])
    Fixture.seed_load_counter(:shared_dashboard_model, user_id)
    Fixture.block_derives_for(user_id, test_pid)

    task_a =
      Task.async(fn ->
        connected_socket()
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
        connected_socket()
        |> Live.watch(:issues, ScopedIssues, user_id: user_id)
        |> Live.watch(:activity, ScopedActivity, user_id: user_id)
        |> Live.derive(
          :dashboard_model,
          [:issues, :activity],
          &__MODULE__.shared_dashboard_model/1
        )
      end)

    TelemetryMessages.assert_counted([:upkeep, :graph, :derived_initial, :hit], %{
      assign_name: :dashboard_model
    })

    send(loader_pid, :continue)

    socket_a = Task.await(task_a)
    socket_b = Task.await(task_b)

    assert socket_a.assigns.dashboard_model == %{
             user_id: user_id,
             issues: [:user_issue],
             activity: [:user_activity]
           }

    assert socket_b.assigns.dashboard_model == socket_a.assigns.dashboard_model
    assert Fixture.load_count(:shared_dashboard_model, user_id) == 1
  end

  test "same-param source modules are colocated for shared multi-source derives" do
    attach_telemetry([[:upkeep, :derive, :sharing]])

    project_id = System.unique_integer([:positive])

    Fixture.seed_source(:issues, project_id, [:project_issue])
    Fixture.seed_source(:activity, project_id, [:project_activity])
    Fixture.seed_load_counter(:shared_project_dashboard_model, project_id)

    source_ids = [
      {ProjectIssues, %{project_id: project_id}},
      {ProjectActivity, %{project_id: project_id}}
    ]

    graph_node_id =
      {:derived, UpkeepWeb.KanbanLive, :project_dashboard_model, source_ids,
       {__MODULE__, :shared_project_dashboard_model, 1}}

    socket =
      connected_socket()
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

    assert Fixture.load_count(:shared_project_dashboard_model, project_id) == 1

    TelemetryMessages.assert_counted(
      [:upkeep, :derive, :sharing],
      %{
        assign_name: :project_dashboard_model,
        result: :shared,
        reason: :shareable,
        graph_node_id: graph_node_id,
        graph_dep_node_ids: source_ids,
        sharing_partition: %{project_id: project_id},
        dep_partitions: [
          {{ProjectIssues, %{project_id: project_id}}, %{project_id: project_id}},
          {{ProjectActivity, %{project_id: project_id}}, %{project_id: project_id}}
        ]
      }
    )
  end

  test "cross-partition multi-source derives fall back locally without leaking values" do
    attach_telemetry([[:upkeep, :derive, :sharing]])

    project_id = System.unique_integer([:positive])
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])

    Fixture.seed_source(:activity, project_id, [:project_activity])

    Fixture.seed_scoped_issues(user_a, [{user_a, :user_a_issue}])
    Fixture.seed_scoped_issues(user_b, [{user_b, :user_b_issue}])
    Fixture.seed_load_counter(:shared_user_project_dashboard_model, user_a)
    Fixture.seed_load_counter(:shared_user_project_dashboard_model, user_b)

    socket_a =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.watch(:activity, ProjectActivity, project_id: project_id)
      |> Live.derive(
        :dashboard_model,
        [:issues, :activity],
        &__MODULE__.shared_user_project_dashboard_model/1
      )

    socket_b =
      connected_socket()
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

    assert Fixture.load_count(:shared_user_project_dashboard_model, user_a) == 1
    assert Fixture.load_count(:shared_user_project_dashboard_model, user_b) == 1

    TelemetryMessages.assert_counted(
      [:upkeep, :derive, :sharing],
      %{
        assign_name: :dashboard_model,
        result: :local,
        reason: :cross_partition_dep,
        dep_partitions: [
          {{ScopedIssues, %{user_id: user_a}}, %{user_id: user_a}},
          {{ProjectActivity, %{project_id: project_id}}, %{project_id: project_id}}
        ]
      }
    )

    TelemetryMessages.assert_counted(
      [:upkeep, :derive, :sharing],
      %{
        assign_name: :dashboard_model,
        result: :local,
        reason: :cross_partition_dep,
        dep_partitions: [
          {{ScopedIssues, %{user_id: user_b}}, %{user_id: user_b}},
          {{ProjectActivity, %{project_id: project_id}}, %{project_id: project_id}}
        ]
      }
    )
  end

  test "connected multi-source derived sharing does not leak values across source params" do
    user_a = System.unique_integer([:positive])
    user_b = System.unique_integer([:positive])

    Fixture.seed_scoped_issues(user_a, [{user_a, :user_a_issue}])
    Fixture.seed_scoped_activity(user_a, [{user_a, :user_a_activity}])
    Fixture.seed_scoped_issues(user_b, [{user_b, :user_b_issue}])
    Fixture.seed_scoped_activity(user_b, [{user_b, :user_b_activity}])
    Fixture.seed_load_counter(:shared_dashboard_model, user_a)
    Fixture.seed_load_counter(:shared_dashboard_model, user_b)

    socket_a =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_a)
      |> Live.watch(:activity, ScopedActivity, user_id: user_a)
      |> Live.derive(:dashboard_model, [:issues, :activity], &__MODULE__.shared_dashboard_model/1)

    socket_b =
      connected_socket()
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

    assert Fixture.load_count(:shared_dashboard_model, user_a) == 1
    assert Fixture.load_count(:shared_dashboard_model, user_b) == 1
  end

  test "graph-pushed source updates reuse shared derived graph values" do
    attach_telemetry([
      [:upkeep, :graph, :dispatch, :stop],
      [:upkeep, :live, :dag_values, :apply],
      [:upkeep, :live, :effects, :apply]
    ])

    user_id = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_id, [{user_id, :before}])
    Fixture.seed_load_counter(:shared_issue_count, user_id)

    socket =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_id)
      |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)

    assert socket.assigns.issue_count == 1
    assert Fixture.load_count(:shared_issue_count, user_id) == 1

    Fixture.set_source_value(:scoped_issues, user_id, [{user_id, :before}, {user_id, :after}])

    socket = assert_shared_issue_graph_push_reused(socket, user_id)

    assert socket.assigns.issues == [{user_id, :before}, {user_id, :after}]
    assert socket.assigns.issue_count == 2
    assert Fixture.load_count(:shared_issue_count, user_id) == 2
  end

  test "local derives recompute after graph-pushed shared derived values" do
    user_id = System.unique_integer([:positive])
    Fixture.seed_scoped_issues(user_id, [{user_id, :before}])
    Fixture.seed_load_counter(:shared_issue_count, user_id)

    extra = user_id

    socket =
      connected_socket()
      |> Live.watch(:issues, ScopedIssues, user_id: user_id)
      |> Live.derive(:issue_count, [:issues], &__MODULE__.shared_issue_count/1)
      |> Live.derive(:visible_count, [:issue_count], fn %{issue_count: count} -> count + extra end)

    assert socket.assigns.visible_count == user_id + 1

    Fixture.set_source_value(:scoped_issues, user_id, [{user_id, :before}, {user_id, :after}])

    socket = assert_shared_issue_push_recomputed_local_derive(socket, user_id)

    assert socket.assigns.issue_count == 2
    assert socket.assigns.visible_count == user_id + 2
    assert Fixture.load_count(:shared_issue_count, user_id) == 2
  end

  test "watch is idempotent for the same source identity" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue_a]
    assert Fixture.load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 1
  end

  test "disconnected LiveView-shaped watches load without joining source interest" do
    socket =
      disconnected_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue_a]
    assert Fixture.load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 0

    _socket = Live.unwatch(socket, :issues)

    assert member_count(ProjectIssues, project_id: 1) == 0
  end

  test "connected LiveView-shaped watches join source interest" do
    socket =
      connected_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue_a]
    assert Fixture.load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 1
  end

  test "duplicate watch can alias the same source value without duplicate membership" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:other_issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue_a]
    assert socket.assigns.other_issues == [:issue_a]
    assert Fixture.load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 1

    Fixture.set_source_value(:issues, 1, [:issue_b])

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_b]
    assert socket.assigns.other_issues == [:issue_b]
    assert Fixture.load_count(:issues) == 2
  end

  test "unwatch by assign keeps shared source interest until last alias is removed" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:other_issues, ProjectIssues, project_id: 1)

    socket = Live.unwatch(socket, :other_issues)

    assert member_count(ProjectIssues, project_id: 1) == 1

    _socket = Live.unwatch(socket, :issues)

    assert member_count(ProjectIssues, project_id: 1) == 0
  end

  test "unwatch by assign leaves interest and stops refreshes" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert member_count(ProjectIssues, project_id: 1) == 1

    socket = Live.unwatch(socket, :issues)

    assert member_count(ProjectIssues, project_id: 1) == 0

    Fixture.set_source_value(:issues, 1, [:issue_b])

    change = updated_issue(1, 1)
    assert :ok = Upkeep.notify(change)
    :ok = Upkeep.Test.drain()
    refute_unwatched_graph_push()

    socket =
      socket
      |> Live.queue_matching(change)
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_a]
    assert Fixture.load_count(:issues) == 1
  end

  test "unwatch by source params clears pending refreshes" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.queue_matching(updated_issue(1, 1))

    Fixture.set_source_value(:issues, 1, [:issue_b])

    socket =
      socket
      |> Live.unwatch(ProjectIssues, project_id: 1)
      |> Live.flush_refreshes()

    assert socket.assigns.issues == [:issue_a]
    assert Fixture.load_count(:issues) == 1
    assert member_count(ProjectIssues, project_id: 1) == 0
  end

  test "derived assigns recompute through a real dependency chain" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], fn %{issues: issues} ->
        Fixture.bump_load({:loads, :issue_count, 1})
        length(issues)
      end)
      |> Live.derive(:issue_label, [:issue_count], fn %{issue_count: count} ->
        Fixture.bump_load({:loads, :issue_label, 1})
        "#{count} issue"
      end)

    assert socket.assigns.issue_count == 1
    assert socket.assigns.issue_label == "1 issue"
    assert Fixture.load_count(:issue_count) == 1
    assert Fixture.load_count(:issue_label) == 1

    Fixture.set_source_value(:issues, 1, [:issue_a, :issue_b])

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_count == 2
    assert socket.assigns.issue_label == "2 issue"
    assert Fixture.load_count(:issue_count) == 2
    assert Fixture.load_count(:issue_label) == 2
  end

  test "shared derived intermediates compute once and unrelated sources do not trigger them" do
    socket =
      socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.watch(:activity, ProjectActivity, project_id: 1)
      |> Live.derive(:visible_issues, [:issues], fn %{issues: issues} ->
        Fixture.bump_load({:loads, :visible, 1})
        issues
      end)
      |> Live.derive(:issue_count, [:visible_issues], fn %{visible_issues: issues} ->
        Fixture.bump_load({:loads, :issue_count, 1})
        length(issues)
      end)
      |> Live.derive(:issue_label, [:visible_issues], fn %{visible_issues: issues} ->
        Fixture.bump_load({:loads, :issue_label, 1})
        List.first(issues)
      end)

    assert socket.assigns.issue_count == 1
    assert socket.assigns.issue_label == :issue_a
    assert Fixture.load_count(:visible) == 1
    assert Fixture.load_count(:issue_count) == 1
    assert Fixture.load_count(:issue_label) == 1

    Fixture.set_source_value(:activity, 1, [:activity_b])

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.activity == [:activity_b]
    assert Fixture.load_count(:visible) == 1
    assert Fixture.load_count(:issue_count) == 1
    assert Fixture.load_count(:issue_label) == 1

    Fixture.set_source_value(:issues, 1, [:issue_a, :issue_b])

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_count == 2
    assert socket.assigns.issue_label == :issue_a
    assert Fixture.load_count(:visible) == 2
    assert Fixture.load_count(:issue_count) == 2
    assert Fixture.load_count(:issue_label) == 2
  end

  test "component-scoped sources enter and leave the graph with downstream derived nodes" do
    socket =
      socket()
      |> Live.component(:issue_detail, [], fn %{} -> %{issue_id: 1} end)
      |> Live.watch(:comments, IssueComments, [issue_id: 1], under: :issue_detail)
      |> Live.derive(:comment_count, [:comments], fn %{comments: comments} -> length(comments) end)

    assert socket.assigns.comments == [:comment_a]
    assert socket.assigns.comment_count == 1
    assert Fixture.load_count(:comments, 1) == 1
    assert member_count(IssueComments, issue_id: 1) == 1

    socket = Live.remove_component(socket, :issue_detail)

    assert member_count(IssueComments, issue_id: 1) == 0

    Fixture.set_source_value(:comments, 1, [:comment_a, :comment_c])

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.comments == [:comment_a]
    assert socket.assigns.comment_count == 1
    assert Fixture.load_count(:comments, 1) == 1

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

  test "function component identity can scope sources without an Upkeep frame" do
    component_id = {__MODULE__.IssueComponents, :issue_card, 1}

    socket =
      socket()
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

    Fixture.set_source_value(:comments, 1, [:comment_a, :comment_c])

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_card_comments == [:comment_a]
    assert socket.assigns.issue_card_comment_count == 1
    assert Fixture.load_count(:comments, 1) == 1
  end

  test "function component input changes remove stale scoped source reads" do
    component_id = {__MODULE__.IssueComponents, :issue_card, 1}
    Fixture.set_source_value(:issues, 1, [1])

    socket =
      socket()
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

    Fixture.set_source_value(:issues, 1, [1, :unchanged_component_value])

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_id == 1
    assert socket.assigns.issue_label == "issue 1"
    assert member_count(IssueComments, [issue_id: 1], component_id) == 1

    Fixture.set_source_value(:issues, 1, [2])

    socket =
      socket
      |> Live.queue_matching(updated_issue(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_id == 2
    assert socket.assigns.issue_label == "issue 2"
    assert member_count(IssueComments, issue_id: 1) == 0

    Fixture.set_source_value(:comments, 1, [:comment_a, :comment_c])

    socket =
      socket
      |> Live.queue_matching(inserted_comment(1, 1))
      |> Live.flush_refreshes()

    assert socket.assigns.issue_card_comments == [:comment_a]
    assert Fixture.load_count(:comments, 1) == 1

    socket =
      Live.watch(socket, :issue_card_comments, IssueComments, [issue_id: 2], under: component_id)

    assert socket.assigns.issue_card_comments == [:comment_b]
    assert member_count(IssueComments, [issue_id: 2], component_id) == 1
  end

  test "removing component-scoped source preserves shared source interest" do
    socket =
      socket()
      |> Live.watch(:comments, IssueComments, issue_id: 1)
      |> Live.component(:issue_detail, [], fn %{} -> %{issue_id: 1} end)
      |> Live.watch(:detail_comments, IssueComments, [issue_id: 1], under: :issue_detail)

    assert member_count(IssueComments, issue_id: 1) == 1
    assert Fixture.load_count(:comments, 1) == 2

    socket = Live.remove_component(socket, :issue_detail)

    assert member_count(IssueComments, issue_id: 1) == 1

    Fixture.set_source_value(:comments, 1, [:comment_a, :comment_c])

    change = inserted_comment(1, 1)
    assert :ok = Upkeep.notify(change)
    assert :ok = Upkeep.Test.drain()

    socket = assert_shared_comment_push(socket, 1, [:comment_a, :comment_c])

    assert socket.assigns.comments == [:comment_a, :comment_c]
    assert Fixture.load_count(:comments, 1) == 3

    _socket = Live.unwatch(socket, :comments)

    assert member_count(IssueComments, issue_id: 1) == 0
  end

  test "graph snapshot exposes assigns, watches, and pending refreshes" do
    socket =
      socket()
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

  test "emits telemetry for watch, shared initial load, queue, reload, recompute, assign, and unwatch" do
    attach_telemetry([
      [:upkeep, :source, :watch],
      [:upkeep, :source, :queue],
      [:upkeep, :source, :reload, :start],
      [:upkeep, :source, :reload, :stop],
      [:upkeep, :source, :unwatch],
      [:upkeep, :dag, :recompute, :start],
      [:upkeep, :dag, :recompute, :stop],
      [:upkeep, :live, :assign],
      [:upkeep, :source, :initial_load, :miss]
    ])

    source_id = {ProjectIssues, %{project_id: 1}}

    socket =
      connected_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], fn %{issues: issues} -> length(issues) end)

    TelemetryMessages.assert_counted([:upkeep, :source, :initial_load, :miss],
      node_id: {:source, source_id},
      source: ProjectIssues,
      params: %{project_id: 1},
      sharing_partition: %{project_id: 1}
    )

    TelemetryMessages.assert_counted([:upkeep, :source, :watch],
      source_id: source_id,
      assign_name: :issues,
      kind: :new,
      sharing_partition: %{project_id: 1}
    )

    TelemetryMessages.assert_counted([:upkeep, :live, :assign],
      assign: :issues,
      node_id: {:source, source_id},
      kind: :source
    )

    Fixture.set_source_value(:issues, 1, [:issue_a, :issue_b])
    change = updated_issue(1, 1)

    socket = Live.queue_matching(socket, change)

    TelemetryMessages.assert_counted([:upkeep, :source, :queue],
      source_id: source_id,
      event: change
    )

    socket = Live.flush_refreshes(socket)

    TelemetryMessages.assert_event([:upkeep, :source, :reload, :start],
      metadata: %{
        source_id: source_id,
        reason: :refresh,
        sharing_partition: %{project_id: 1}
      }
    )

    {measurements, _metadata} =
      TelemetryMessages.assert_event([:upkeep, :source, :reload, :stop],
        metadata: %{
          source_id: source_id,
          reason: :refresh,
          sharing_partition: %{project_id: 1}
        }
      )

    assert is_integer(measurements.duration)

    TelemetryMessages.assert_event([:upkeep, :dag, :recompute, :stop],
      metadata: %{
        changed_source_nodes: [{:source, source_id}],
        changed_derived_nodes: [{:derived, :issue_count}],
        recomputed_nodes: [{:derived, :issue_count}],
        changed_count: 1,
        recomputed_count: 1
      }
    )

    TelemetryMessages.assert_counted([:upkeep, :live, :assign],
      assign: :issue_count,
      node_id: {:derived, :issue_count},
      kind: :derived
    )

    _socket = Live.unwatch(socket, :issues)

    TelemetryMessages.assert_counted([:upkeep, :source, :unwatch],
      source_id: source_id,
      kind: :remove
    )
  end

  def shared_issue_count(%{issues: [{user_id, _name} | _] = issues}) do
    Fixture.bump_load({:loads, :shared_issue_count, user_id})

    block_derive_if_configured(
      user_id,
      :derived_compute_started,
      "blocking derived compute was not released"
    )

    length(issues)
  end

  def shared_local_issue_count(%{local_issues: issues}), do: length(issues)

  def shared_issue_names(%{issues: [{user_id, _name} | _] = issues}) do
    Fixture.bump_load({:loads, :shared_issue_names, user_id})
    Enum.map(issues, fn {_user_id, name} -> name end)
  end

  def shared_issue_stats(%{issues: [{user_id, _name} | _] = issues}) do
    Fixture.bump_load({:loads, :shared_issue_stats, user_id})

    block_derive_if_configured(
      user_id,
      :derived_compute_started,
      "blocking derived compute was not released"
    )

    %{user_id: user_id, count: length(issues)}
  end

  def shared_issue_label(%{issue_stats: %{user_id: user_id, count: count}}) do
    Fixture.bump_load({:loads, :shared_issue_label, user_id})

    block_derive_if_configured(
      user_id,
      :derived_label_started,
      "blocking derived label compute was not released"
    )

    "#{count} issue"
  end

  def shared_user_label(%{issue_stats: %{user_id: user_id, count: count}}) do
    Fixture.bump_load({:loads, :shared_user_label, user_id})
    "user #{user_id}: #{count} issue"
  end

  def shared_dashboard_model(%{issues: [{user_id, _issue} | _] = issues, activity: activity}) do
    Fixture.bump_load({:loads, :shared_dashboard_model, user_id})

    block_derive_if_configured(
      user_id,
      :dashboard_model_started,
      "blocking dashboard model compute was not released"
    )

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
      case Fixture.project_id_for_issues(issues) do
        {:ok, id} -> id
        :error -> project_id
      end

    Fixture.bump_load({:loads, :shared_project_dashboard_model, project_id})

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
    Fixture.bump_load({:loads, :shared_user_project_dashboard_model, user_id})

    %{
      user_id: user_id,
      issues: Enum.map(issues, fn {_user_id, issue} -> issue end),
      activity: activity
    }
  end

  defp assert_shared_issue_graph_push_reused(socket, user_id) do
    issues = [{user_id, :before}, {user_id, :after}]
    source_id = {ScopedIssues, %{user_id: user_id}}
    graph_node_id = shared_issue_count_node_id(source_id)

    assert :ok =
             %Issue{issue_id: user_id}
             |> Upkeep.Change.updated()
             |> Upkeep.notify()

    assert :ok = Upkeep.Test.drain()

    pairs = DagMessages.receive_batch()
    assert {source_id, issues} in pairs
    assert {graph_node_id, 2} in pairs

    TelemetryMessages.assert_event([:upkeep, :graph, :dispatch, :stop],
      metadata: %{
        node_partitions: [
          {source_id, %{user_id: user_id}},
          {graph_node_id, %{user_id: user_id}}
        ]
      }
    )

    flush_telemetry_messages()
    socket = Live.apply_dag_values(socket, pairs)
    assert_shared_issue_push_apply_telemetry()
    socket
  end

  defp assert_shared_issue_push_recomputed_local_derive(socket, user_id) do
    source_id = {ScopedIssues, %{user_id: user_id}}
    graph_node_id = shared_issue_count_node_id(source_id)

    assert :ok =
             %Issue{issue_id: user_id}
             |> Upkeep.Change.updated()
             |> Upkeep.notify()

    pairs = DagMessages.receive_batch()
    assert {graph_node_id, 2} in pairs
    Live.apply_dag_values(socket, pairs)
  end

  defp assert_shared_comment_push(socket, issue_id, comments) do
    source_id = {IssueComments, %{issue_id: issue_id}}
    assert DagMessages.receive_value(source_id) == comments
    Live.apply_dag_value(socket, source_id, comments)
  end

  defp refute_unwatched_graph_push do
    DagMessages.refute_any()
  end

  defp assert_shared_issue_push_apply_telemetry do
    {%{duration: apply_duration},
     %{
       changed_node_count: changed_node_count,
       shared_node_count: shared_node_count,
       effect_count: effect_count,
       assign_effect_count: assign_effect_count,
       recompute_effect_count: recompute_effect_count
     }} =
      TelemetryMessages.assert_counted([:upkeep, :live, :dag_values, :apply], pair_count: 2)

    assert is_integer(apply_duration)
    assert changed_node_count >= 1
    assert shared_node_count >= 1
    assert effect_count >= 2
    assert assign_effect_count >= 2
    assert recompute_effect_count >= 0

    {%{duration: effects_duration},
     %{
       effect_count: materialized_effect_count,
       assign_count: materialized_assign_count,
       telemetry_count: materialized_telemetry_count
     }} =
      TelemetryMessages.assert_counted([:upkeep, :live, :effects, :apply])

    assert is_integer(effects_duration)
    assert materialized_effect_count >= 2
    assert materialized_assign_count >= 2
    assert materialized_telemetry_count >= 2
  end

  defp shared_issue_count_node_id(source_id) do
    {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id],
     {__MODULE__, :shared_issue_count, 1}}
  end

  defp block_derive_if_configured(user_id, event, message) do
    case Fixture.derive_blocker(user_id) do
      {:ok, test_pid} ->
        send(test_pid, {event, self(), user_id})

        receive do
          :continue -> :ok
        after
          1_000 -> raise message
        end

      :error ->
        :ok
    end
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
    instance = Upkeep.Source.instance(source, params)

    source_ids = [
      Upkeep.Runtime.Ids.scoped_source_id(instance, nil),
      Upkeep.Runtime.Ids.scoped_source_id(instance, component)
    ]

    subscribed? =
      Enum.any?(source_ids, fn source_id ->
        Upkeep.Test.subscribed?(source_id, self())
      end)

    if subscribed?, do: 1, else: 0
  end

  defp flush_telemetry_messages do
    receive do
      {:telemetry, _event, _measurements, _metadata} -> flush_telemetry_messages()
    after
      0 -> :ok
    end
  end
end
