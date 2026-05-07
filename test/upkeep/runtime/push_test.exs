defmodule Upkeep.Runtime.PushTest do
  use ExUnit.Case, async: true

  import Upkeep.TestSupport, only: [attach_telemetry: 1]

  defmodule ProjectIssues do
  end

  test "unknown pushed nodes are ignored with reasons" do
    attach_telemetry([
      [:upkeep, :live, :dag_values, :apply],
      [:upkeep, :live, :dag_values, :ignored]
    ])

    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}, issues: [:current]}}
    source_id = {ProjectIssues, %{project_id: 1}}

    shared_id =
      {:derived, UpkeepWeb.KanbanLive, :issue_count, [source_id], {__MODULE__, :count, 1}}

    stale_id = {:unknown, :node}

    assert {:ok, ^socket, []} =
             Upkeep.Runtime.apply_dag_values(socket, [
               {source_id, [:pushed]},
               {shared_id, 1},
               {stale_id, :value}
             ])

    assert_receive {:telemetry, [:upkeep, :live, :dag_values, :apply], _measurements,
                    %{
                      pair_count: 3,
                      changed_node_count: 0,
                      effect_count: 0,
                      ignored_count: 3,
                      ignored_reason_counts: %{
                        unwatched_source: 1,
                        unknown_shared_derived: 1,
                        stale_push: 1
                      }
                    }}

    assert_ignored(:unwatched_source, source_id)
    assert_ignored(:unknown_shared_derived, shared_id)
    assert_ignored(:stale_push, stale_id)
  end

  test "single unknown pushed value emits ignored telemetry" do
    attach_telemetry([[:upkeep, :live, :dag_values, :ignored]])

    socket = %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
    source_id = {ProjectIssues, %{project_id: 2}}

    assert {:ok, ^socket, []} = Upkeep.Runtime.apply_dag_value(socket, source_id, [:pushed])

    assert_ignored(:unwatched_source, source_id)
  end

  defp assert_ignored(reason, node_id) do
    assert_receive {:telemetry, [:upkeep, :live, :dag_values, :ignored], %{count: 1},
                    %{reason: ^reason, node_id: ^node_id, node_ids: [^node_id]}}
  end
end
