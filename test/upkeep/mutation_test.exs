defmodule Upkeep.Internal.MutationTest do
  use Upkeep.DataCase, async: false

  alias Upkeep.Live
  import ExUnit.CaptureLog

  defmodule Issue do
    defstruct [:project_id, :issue_id]
  end

  defmodule WarningIssue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    query(fn s ->
      [{_key, value}] = :ets.lookup(Upkeep.Internal.MutationTest, {:issues, s.project_id})
      value
    end)

    invalidated_by(Issue, :updated, on: :project_id)
  end

  setup do
    if :ets.info(__MODULE__) != :undefined do
      :ets.delete(__MODULE__)
    end

    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:issues, 777}, [:before]})
    :ets.insert(table, {{:issues, 778}, [:before]})
    :ets.insert(table, {{:issues, 779}, [:before]})
    :ets.insert(table, {{:issues, 780}, [:before]})

    on_exit(fn ->
      drain_graph()

      if :ets.info(__MODULE__) != :undefined do
        :ets.delete(__MODULE__)
      end
    end)

    %{table: table}
  end

  test "mutate flushes notifications after commit", %{table: table} do
    socket = watch_project(777)

    result =
      Upkeep.mutate(fn ->
        :ets.insert(table, {{:issues, 777}, [:after]})
        Upkeep.updated(issue(777, 1))
        :moved
      end)

    assert {:ok, :moved} = result
    drain_graph()

    source_id = source_id(777)
    assert_receive {:dag_values, [{^source_id, [:after]}]}

    refreshed = Live.apply_dag_value(socket, source_id, [:after])
    assert refreshed.assigns.issues == [:after]
  end

  test "mutate discards notifications on rollback" do
    watch_project(778)

    result =
      Upkeep.mutate(fn ->
        Upkeep.updated(issue(778, 1))
        Upkeep.Repo.rollback(:cancelled)
      end)

    assert {:error, :cancelled} = result
    drain_graph()
    refute_received {:dag_values, [{{ProjectIssues, %{project_id: 778}}, _}]}
  end

  test "mutate discards notifications when the mutation raises" do
    watch_project(779)

    assert_raise RuntimeError, "boom", fn ->
      Upkeep.mutate(fn ->
        Upkeep.updated(issue(779, 1))
        raise "boom"
      end)
    end

    drain_graph()
    refute_received {:dag_values, [{{ProjectIssues, %{project_id: 779}}, _}]}
  end

  test "mutate preserves notification order after commit" do
    watch_project(780)

    result =
      Upkeep.mutate(fn ->
        Upkeep.updated(issue(780, 1))
        Upkeep.updated(issue(780, 2))
        :ok
      end)

    assert {:ok, :ok} = result
    drain_graph()

    assert_receive {:dag_values, [{{ProjectIssues, %{project_id: 780}}, [:before]}]}
    refute_received {:dag_values, [{{ProjectIssues, %{project_id: 780}}, _}]}
  end

  test "nested mutate joins the outer journal and flushes once" do
    watch_project(777)

    result =
      Upkeep.mutate(fn ->
        assert {:ok, :inner} =
                 Upkeep.mutate(fn ->
                   Upkeep.updated(issue(777, 1))
                   :inner
                 end)

        refute_received {:dag_values, [{{ProjectIssues, %{project_id: 777}}, _}]}
        :outer
      end)

    assert {:ok, :outer} = result
    drain_graph()

    assert_receive {:dag_values, [{{ProjectIssues, %{project_id: 777}}, [:before]}]}
  end

  test "Ecto.Multi flushes notifications after commit" do
    watch_project(777)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:move, fn _repo, _changes ->
        Upkeep.updated(issue(777, 1))
        {:ok, :moved}
      end)

    assert {:ok, %{move: :moved}} = Upkeep.mutate(multi)
    drain_graph()

    assert_receive {:dag_values, [{{ProjectIssues, %{project_id: 777}}, [:before]}]}
  end

  test "Ecto.Multi discards notifications on rollback" do
    watch_project(778)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:move, fn _repo, _changes ->
        Upkeep.updated(issue(778, 1))
        {:error, :cancelled}
      end)

    assert {:error, :move, :cancelled, %{}} = Upkeep.mutate(multi)
    drain_graph()

    refute_received {:dag_values, [{{ProjectIssues, %{project_id: 778}}, _}]}
  end

  test "Ecto.Multi preserves notification order after commit" do
    watch_project(779)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:first, fn _repo, _changes ->
        Upkeep.updated(issue(779, 1))
        {:ok, :first}
      end)
      |> Ecto.Multi.run(:second, fn _repo, _changes ->
        Upkeep.updated(issue(779, 2))
        {:ok, :second}
      end)

    assert {:ok, %{first: :first, second: :second}} = Upkeep.mutate(multi)
    drain_graph()

    assert_receive {:dag_values, [{{ProjectIssues, %{project_id: 779}}, [:before]}]}
    refute_received {:dag_values, [{{ProjectIssues, %{project_id: 779}}, _}]}
  end

  test "nested Ecto.Multi rollback rolls back the outer mutation and discards all events" do
    watch_project(780)

    result =
      Upkeep.mutate(fn ->
        Upkeep.updated(issue(780, 1))

        multi =
          Ecto.Multi.new()
          |> Ecto.Multi.run(:inner, fn _repo, _changes ->
            Upkeep.updated(issue(780, 2))
            {:error, :cancelled}
          end)

        assert {:error, :inner, :cancelled, %{}} = Upkeep.mutate(multi)
        :outer
      end)

    assert {:error, :rollback} = result
    drain_graph()

    refute_received {:dag_values, [{{ProjectIssues, %{project_id: 780}}, _}]}
  end

  test "updates without old state emit a broad-invalidation diagnostic" do
    Upkeep.clear_events()

    log =
      capture_log(fn ->
        assert :ok = Upkeep.notify(Upkeep.Change.updated(%WarningIssue{project_id: 123}))
        drain_graph()
        _ = :sys.get_state(Upkeep.Observability)
      end)

    assert log =~ "without `from: old_record`"
    assert log =~ "refresh all matching `:updated` sources"

    assert Enum.any?(Upkeep.recent_events(), fn event ->
             event.event == [:upkeep, :change, :broad_update] and
               event.metadata.schema == WarningIssue and
               event.metadata.reason == :missing_old_state and
               event.metadata.policy == :warn
           end)
  end

  defp watch_project(project_id) do
    %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    |> Live.watch(:issues, ProjectIssues, project_id: project_id)
  end

  defp source_id(project_id), do: {ProjectIssues, %{project_id: project_id}}

  defp issue(project_id, issue_id), do: %Issue{project_id: project_id, issue_id: issue_id}

  defp drain_graph do
    :ok = Upkeep.Internal.Coordinator.Graph.drain()
  end
end
