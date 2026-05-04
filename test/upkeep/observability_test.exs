defmodule Upkeep.ObservabilityTest do
  use ExUnit.Case, async: false

  alias Upkeep.Live

  defmodule Issue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    query(fn s ->
      [{_key, value}] = :ets.lookup(Upkeep.ObservabilityTest, {:issues, s.project_id})
      value
    end)

    invalidated_by(Issue, :updated, on: :project_id)
  end

  setup do
    Upkeep.Test.reset_graph()

    if :ets.info(__MODULE__) != :undefined do
      :ets.delete(__MODULE__)
    end

    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:issues, 1}, [:before]})
    Upkeep.clear_events()

    on_exit(fn ->
      Upkeep.clear_events()

      if :ets.info(__MODULE__) != :undefined do
        :ets.delete(__MODULE__)
      end
    end)

    %{table: table}
  end

  test "stores recent runtime telemetry events", %{table: table} do
    socket =
      %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], fn %{issues: issues} -> length(issues) end)

    :ets.insert(table, {{:issues, 1}, [:before, :after]})
    change = %Issue{project_id: 1} |> Upkeep.Change.updated()

    socket =
      socket
      |> Live.queue_matching(change)
      |> Live.flush_refreshes()

    assert socket.assigns.issue_count == 2

    events = Upkeep.recent_events()
    event_names = Enum.map(events, & &1.event)

    assert [:upkeep, :source, :watch] in event_names
    assert [:upkeep, :source, :queue] in event_names
    assert [:upkeep, :source, :reload, :stop] in event_names
    assert [:upkeep, :dag, :recompute, :stop] in event_names
    assert [:upkeep, :live, :assign] in event_names

    assert Enum.all?(events, &is_integer(&1.at))

    assert Enum.any?(events, fn event ->
             event.event == [:upkeep, :source, :queue] and
               event.measurements == %{count: 1} and
               event.metadata.source == ProjectIssues
           end)
  end

  test "stores Graph dispatch telemetry events", %{table: table} do
    project_id = System.unique_integer([:positive])
    :ets.insert(table, {{:issues, project_id}, [:before]})

    %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    |> Live.watch(:issues, ProjectIssues, project_id: project_id)

    :ets.insert(table, {{:issues, project_id}, [:after]})

    assert :ok =
             %Issue{project_id: project_id}
             |> Upkeep.Change.updated()
             |> Upkeep.notify()

    assert :ok = Upkeep.Coordinator.Graph.drain()
    assert_receive {:dag_values, [{{ProjectIssues, %{project_id: ^project_id}}, [:after]}]}

    events = recent_events_after_observability_flush()

    assert Enum.any?(events, fn event ->
             event.event == [:upkeep, :graph, :dispatch, :stop] and
               Map.get(event.metadata, :pair_count, 0) >= 1 and
               Map.get(event.metadata, :pid_count, 0) >= 1 and
               is_integer(event.measurements.duration)
           end)
  end

  test "stores derive sharing diagnostics", %{table: table} do
    project_id = System.unique_integer([:positive])
    :ets.insert(table, {{:issues, project_id}, [:before]})

    %Phoenix.LiveView.Socket{
      endpoint: UpkeepWeb.Endpoint,
      view: UpkeepWeb.KanbanLive,
      transport_pid: self(),
      assigns: %{__changed__: %{}}
    }
    |> Live.watch(:issues, ProjectIssues, project_id: project_id)
    |> Live.derive(:issue_count, [:issues], &__MODULE__.issue_count/1)
    |> Live.derive(:local_count, [:issue_count], fn %{issue_count: count} -> count end)

    events = recent_events_after_observability_flush()

    assert Enum.any?(events, fn event ->
             event.event == [:upkeep, :derive, :sharing] and
               event.metadata.assign_name == :issue_count and
               event.metadata.result == :shared and
               event.metadata.reason == :shareable and
               event.metadata.fun == {__MODULE__, :issue_count, 1}
           end)

    assert Enum.any?(events, fn event ->
             event.event == [:upkeep, :derive, :sharing] and
               event.metadata.assign_name == :local_count and
               event.metadata.result == :local and
               event.metadata.reason == :local_fun
           end)
  end

  test "can clear stored events" do
    %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert Upkeep.recent_events() != []
    assert :ok = Upkeep.clear_events()
    assert Upkeep.recent_events() == []
  end

  defp recent_events_after_observability_flush do
    _ = :sys.get_state(Upkeep.Observability)
    Upkeep.recent_events()
  end

  def issue_count(%{issues: issues}), do: length(issues)
end
