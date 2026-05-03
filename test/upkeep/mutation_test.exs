defmodule Upkeep.MutationTest do
  use Upkeep.DataCase, async: false

  alias Upkeep.Live

  defmodule IssueMoved do
    defstruct [:project_id, :issue_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    query(fn s ->
      [{_key, value}] = :ets.lookup(Upkeep.MutationTest, {:issues, s.project_id})
      value
    end)

    invalidated_by(IssueMoved, on: :project_id)
  end

  setup do
    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:issues, 777}, [:before]})
    :ets.insert(table, {{:issues, 778}, [:before]})
    :ets.insert(table, {{:issues, 779}, [:before]})
    :ets.insert(table, {{:issues, 780}, [:before]})

    %{table: table}
  end

  test "mutate flushes notifications after commit", %{table: table} do
    socket = watch_project(777)

    result =
      Upkeep.mutate(fn ->
        :ets.insert(table, {{:issues, 777}, [:after]})
        Upkeep.notify(%IssueMoved{project_id: 777, issue_id: 1})
        :moved
      end)

    assert {:ok, :moved} = result
    assert_receive {:upkeep_event, %IssueMoved{project_id: 777}}

    refreshed = Live.refresh_matching(socket, %IssueMoved{project_id: 777, issue_id: 1})
    assert refreshed.assigns.issues == [:after]
  end

  test "mutate discards notifications on rollback" do
    watch_project(778)

    result =
      Upkeep.mutate(fn ->
        Upkeep.notify(%IssueMoved{project_id: 778, issue_id: 1})
        Upkeep.Repo.rollback(:cancelled)
      end)

    assert {:error, :cancelled} = result
    refute_receive {:upkeep_event, %IssueMoved{project_id: 778}}
  end

  test "mutate discards notifications when the mutation raises" do
    watch_project(779)

    assert_raise RuntimeError, "boom", fn ->
      Upkeep.mutate(fn ->
        Upkeep.notify(%IssueMoved{project_id: 779, issue_id: 1})
        raise "boom"
      end)
    end

    refute_receive {:upkeep_event, %IssueMoved{project_id: 779}}
  end

  test "mutate preserves notification order after commit" do
    watch_project(780)

    result =
      Upkeep.mutate(fn ->
        Upkeep.notify(%IssueMoved{project_id: 780, issue_id: 1})
        Upkeep.notify(%IssueMoved{project_id: 780, issue_id: 2})
        :ok
      end)

    assert {:ok, :ok} = result
    assert_receive {:upkeep_event, %IssueMoved{project_id: 780, issue_id: 1}}
    assert_receive {:upkeep_event, %IssueMoved{project_id: 780, issue_id: 2}}
  end

  test "nested mutate joins the outer journal and flushes once" do
    watch_project(777)

    result =
      Upkeep.mutate(fn ->
        assert {:ok, :inner} =
                 Upkeep.mutate(fn ->
                   Upkeep.notify(%IssueMoved{project_id: 777, issue_id: 1})
                   :inner
                 end)

        refute_received {:upkeep_event, %IssueMoved{project_id: 777}}
        :outer
      end)

    assert {:ok, :outer} = result
    assert_receive {:upkeep_event, %IssueMoved{project_id: 777, issue_id: 1}}
  end

  test "Ecto.Multi flushes notifications after commit" do
    watch_project(777)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:move, fn _repo, _changes ->
        Upkeep.notify(%IssueMoved{project_id: 777, issue_id: 1})
        {:ok, :moved}
      end)

    assert {:ok, %{move: :moved}} = Upkeep.mutate(multi)
    assert_receive {:upkeep_event, %IssueMoved{project_id: 777, issue_id: 1}}
  end

  test "Ecto.Multi discards notifications on rollback" do
    watch_project(778)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:move, fn _repo, _changes ->
        Upkeep.notify(%IssueMoved{project_id: 778, issue_id: 1})
        {:error, :cancelled}
      end)

    assert {:error, :move, :cancelled, %{}} = Upkeep.mutate(multi)
    refute_receive {:upkeep_event, %IssueMoved{project_id: 778}}
  end

  test "Ecto.Multi preserves notification order after commit" do
    watch_project(779)

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:first, fn _repo, _changes ->
        Upkeep.notify(%IssueMoved{project_id: 779, issue_id: 1})
        {:ok, :first}
      end)
      |> Ecto.Multi.run(:second, fn _repo, _changes ->
        Upkeep.notify(%IssueMoved{project_id: 779, issue_id: 2})
        {:ok, :second}
      end)

    assert {:ok, %{first: :first, second: :second}} = Upkeep.mutate(multi)
    assert_receive {:upkeep_event, %IssueMoved{project_id: 779, issue_id: 1}}
    assert_receive {:upkeep_event, %IssueMoved{project_id: 779, issue_id: 2}}
  end

  test "nested Ecto.Multi rollback rolls back the outer mutation and discards all events" do
    watch_project(780)

    result =
      Upkeep.mutate(fn ->
        Upkeep.notify(%IssueMoved{project_id: 780, issue_id: 1})

        multi =
          Ecto.Multi.new()
          |> Ecto.Multi.run(:inner, fn _repo, _changes ->
            Upkeep.notify(%IssueMoved{project_id: 780, issue_id: 2})
            {:error, :cancelled}
          end)

        assert {:error, :inner, :cancelled, %{}} = Upkeep.mutate(multi)
        :outer
      end)

    assert {:error, :rollback} = result
    refute_receive {:upkeep_event, %IssueMoved{project_id: 780, issue_id: 1}}
    refute_receive {:upkeep_event, %IssueMoved{project_id: 780, issue_id: 2}}
  end

  defp watch_project(project_id) do
    %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    |> Live.watch(:issues, ProjectIssues, project_id: project_id)
  end
end
