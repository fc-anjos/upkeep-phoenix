defmodule Upkeep.MutationTest do
  use Upkeep.TestSupport.DataCase, async: false

  alias Upkeep.Live
  alias Upkeep.TestSupport.{DagMessages, LiveSocket}
  alias Upkeep.TestSupport.Repo
  import ExUnit.CaptureLog

  defmodule Issue do
    defstruct [:project_id, :issue_id]
  end

  defmodule WarningIssue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    def load(s) do
      [{_key, value}] = :ets.lookup(Upkeep.MutationTest, {:issues, s.project_id})
      value
    end

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
      await_graph_idle()

      if :ets.info(__MODULE__) != :undefined do
        :ets.delete(__MODULE__)
      end
    end)

    %{table: table}
  end

  test "with_upkeep toggles the process capture default and restores it" do
    assert Upkeep.Mutation.capture_default() == true

    result =
      Upkeep.with_upkeep(false, fn ->
        assert Upkeep.Mutation.capture_default() == false

        Upkeep.with_upkeep(true, fn ->
          assert Upkeep.Mutation.capture_default() == true
        end)

        assert Upkeep.Mutation.capture_default() == false
        :done
      end)

    assert result == :done
    assert Upkeep.Mutation.capture_default() == true
  end

  test "with_upkeep restores the capture default even when the block raises" do
    assert Upkeep.Mutation.capture_default() == true

    assert_raise RuntimeError, "boom", fn ->
      Upkeep.with_upkeep(false, fn -> raise "boom" end)
    end

    assert Upkeep.Mutation.capture_default() == true
  end

  test "mutate flushes notifications after commit", %{table: table} do
    socket = watch_project(777)

    result =
      Upkeep.Test.sync(fn ->
        Upkeep.mutate(fn ->
          :ets.insert(table, {{:issues, 777}, [:after]})
          Upkeep.updated(issue(777, 1))
          :moved
        end)
      end)

    assert {:ok, :moved} = result

    refreshed = assert_project_refreshed(socket, 777, [:after])
    assert refreshed.assigns.issues == [:after]
  end

  test "await_idle synchronizes pending graph notifications", %{table: table} do
    socket = watch_project(777)

    result =
      Upkeep.mutate(fn ->
        :ets.insert(table, {{:issues, 777}, [:after]})
        Upkeep.updated(issue(777, 1))
        :moved
      end)

    assert {:ok, :moved} = result
    await_graph_idle()

    refreshed = assert_project_refreshed(socket, 777, [:after])
    assert refreshed.assigns.issues == [:after]
  end

  test "mutate discards notifications on rollback" do
    watch_project(778)

    result =
      Upkeep.Test.sync(fn ->
        Upkeep.mutate(fn ->
          Upkeep.updated(issue(778, 1))
          Repo.rollback(:cancelled)
        end)
      end)

    assert {:error, :cancelled} = result
    refute_project_refresh(778)
  end

  test "mutate discards notifications when the mutation raises" do
    watch_project(779)

    Upkeep.Test.sync(fn ->
      assert_raise RuntimeError, "boom", fn ->
        Upkeep.mutate(fn ->
          Upkeep.updated(issue(779, 1))
          raise "boom"
        end)
      end
    end)

    refute_project_refresh(779)
  end

  test "mutate preserves notification order after commit" do
    watch_project(780)

    result =
      Upkeep.Test.sync(fn ->
        Upkeep.mutate(fn ->
          Upkeep.updated(issue(780, 1))
          Upkeep.updated(issue(780, 2))
          :ok
        end)
      end)

    assert {:ok, :ok} = result

    assert_project_refresh(780, [:before])
    refute_project_refresh(780)
  end

  test "nested mutate joins the outer journal and flushes once" do
    watch_project(777)

    result =
      Upkeep.Test.sync(fn ->
        Upkeep.mutate(fn ->
          assert {:ok, :inner} =
                   Upkeep.mutate(fn ->
                     Upkeep.updated(issue(777, 1))
                     :inner
                   end)

          refute_project_refresh(777)
          :outer
        end)
      end)

    assert {:ok, :outer} = result

    assert_project_refresh(777, [:before])
  end

  test "Ecto.Multi flushes notifications after commit" do
    watch_project(777)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:move, fn _repo, _changes ->
        Upkeep.updated(issue(777, 1))
        {:ok, :moved}
      end)

    assert {:ok, %{move: :moved}} = Upkeep.Test.sync(fn -> Upkeep.mutate(multi) end)

    assert_project_refresh(777, [:before])
  end

  test "Ecto.Multi discards notifications on rollback" do
    watch_project(778)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:move, fn _repo, _changes ->
        Upkeep.updated(issue(778, 1))
        {:error, :cancelled}
      end)

    assert {:error, :move, :cancelled, %{}} =
             Upkeep.Test.sync(fn -> Upkeep.mutate(multi) end)

    refute_project_refresh(778)
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

    assert {:ok, %{first: :first, second: :second}} =
             Upkeep.Test.sync(fn -> Upkeep.mutate(multi) end)

    assert_project_refresh(779, [:before])
    refute_project_refresh(779)
  end

  test "nested Ecto.Multi rollback rolls back the outer mutation and discards all events" do
    watch_project(780)

    result =
      Upkeep.Test.sync(fn ->
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
      end)

    assert {:error, :rollback} = result
    refute_project_refresh(780)
  end

  test "updates without old state emit a broad-invalidation diagnostic" do
    Upkeep.clear_events()

    log =
      capture_log(fn ->
        Upkeep.Test.sync(fn ->
          assert :ok = Upkeep.notify(Upkeep.Change.updated(%WarningIssue{project_id: 123}))
        end)

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
    LiveSocket.socket()
    |> Live.watch(:issues, ProjectIssues, project_id: project_id)
  end

  defp assert_project_refreshed(socket, project_id, issues) do
    assert_project_refresh(project_id, issues)
    Live.apply_dag_value(socket, source_id(project_id), issues)
  end

  defp assert_project_refresh(project_id, issues) do
    assert DagMessages.receive_value(source_id(project_id)) == issues
  end

  defp refute_project_refresh(project_id) do
    DagMessages.refute_value(source_id(project_id), 0)
  end

  defp source_id(project_id), do: {ProjectIssues, %{project_id: project_id}}

  defp issue(project_id, issue_id), do: %Issue{project_id: project_id, issue_id: issue_id}

  defp await_graph_idle do
    :ok = Upkeep.Test.await_idle()
  end
end
