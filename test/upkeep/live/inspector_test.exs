defmodule Upkeep.Live.InspectorTest do
  use ExUnit.Case, async: false

  alias Upkeep.Live
  alias Upkeep.Live.Inspector
  alias Upkeep.Live.ScopeHook

  defmodule Issue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    query(fn _params ->
      [%{id: 1, title: "Secret issue"}]
    end)

    invalidated_by(Issue, :updated, on: :project_id)
  end

  defmodule Calculations do
    def issue_count(%{issues: issues}), do: length(issues)
  end

  defmodule MacroDeclarations do
    import Upkeep.Live.Macros

    alias Upkeep.Live.InspectorTest.{Calculations, ProjectIssues}

    def mount(socket) do
      socket
      |> watch(:issues, ProjectIssues, project_id: 1)
      |> derive(:issue_count, [:issues], &Calculations.issue_count/1)
    end
  end

  setup do
    Upkeep.clear_events()

    on_exit(fn -> Upkeep.clear_events() end)
  end

  test "query params switch the LiveView render function into inspector mode" do
    {:cont, socket} =
      ScopeHook.on_mount([inspector: true], %{"_upkeep" => "dag"}, %{}, new_socket())

    assert socket.assigns.current_scope == nil
    assert socket.assigns.upkeep_inspector? == true
    assert socket.assigns.upkeep_inspector_mode == :dag
    assert is_function(socket.private.render_with, 1)
  end

  test "inspector can be disabled for a LiveView" do
    {:cont, socket} =
      ScopeHook.on_mount([inspector: false], %{"_upkeep" => "dag"}, %{}, new_socket())

    assert socket.assigns.current_scope == nil
    refute Map.has_key?(socket.assigns, :upkeep_inspector?)
    refute Map.has_key?(socket.private, :render_with)
  end

  test "renders a symbolic DAG without leaking assign values" do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], &Calculations.issue_count/1)

    document = Upkeep.introspection_snapshot(socket)

    assert Enum.any?(document.assigns, &(&1.label == "@issues"))
    assert Enum.any?(document.assigns, &(&1.label == "@issue_count"))
    assert Enum.any?(document.dag.nodes, &(&1.kind == :source))
    assert Enum.any?(document.dag.nodes, &(&1.kind == :derived))

    html =
      socket.assigns
      |> Map.put(:socket, socket)
      |> Inspector.render()
      |> Phoenix.HTML.Safe.to_iodata()
      |> IO.iodata_to_binary()

    assert html =~ ~s(id="upkeep-inspector")
    assert html =~ ~s(id="upkeep-dag-svg")
    assert html =~ "@issues"
    assert html =~ "ProjectIssues"
    assert html =~ "list(1) of"
    refute html =~ "Secret issue"
  end

  test "reads assign shapes from render assigns when LiveView hides socket assigns" do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    hidden_socket = %{
      socket
      | assigns: %Phoenix.LiveView.Socket.AssignsNotInSocket{__assigns__: socket.assigns}
    }

    html =
      socket.assigns
      |> Map.put(:socket, hidden_socket)
      |> Inspector.render()
      |> Phoenix.HTML.Safe.to_iodata()
      |> IO.iodata_to_binary()

    assert html =~ "list(1) of"
    refute html =~ "Secret issue"
  end

  test "captures DSL callsite source for inspector nodes" do
    socket = MacroDeclarations.mount(new_socket())
    document = Upkeep.introspection_snapshot(socket)

    source_node = Enum.find(document.dag.nodes, &(&1.kind == :source))
    derived_node = Enum.find(document.dag.nodes, &(&1.kind == :derived))

    assert source_node.source_location.location_label =~ "inspector_test.exs:"
    assert source_node.source_location.code =~ "|> watch(:issues, ProjectIssues"
    assert derived_node.source_location.code =~ "|> derive(:issue_count, [:issues]"

    html =
      socket.assigns
      |> Map.put(:socket, socket)
      |> Inspector.render()
      |> Phoenix.HTML.Safe.to_iodata()
      |> IO.iodata_to_binary()

    assert html =~ "Captured Source"
    assert html =~ "|&gt; watch(:issues, ProjectIssues"
    refute html =~ "Secret issue"
  end

  test "renders inspector panels with telemetry driven node state" do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)
      |> Live.derive(:issue_count, [:issues], &Calculations.issue_count/1)

    socket = %{socket | assigns: Map.put(socket.assigns, :__changed__, %{issue_count: true})}

    source_node = {:source, {ProjectIssues, %{project_id: 1}}}
    derived_node = {:derived, :issue_count}

    :telemetry.execute(
      [:upkeep, :dag, :recompute, :stop],
      %{duration: 100},
      %{
        changed_source_nodes: [source_node],
        changed_derived_nodes: [derived_node],
        recomputed_nodes: [derived_node],
        skipped_nodes: [],
        changed_count: 1,
        recomputed_count: 1
      }
    )

    _ = :sys.get_state(Upkeep.Observability)

    html =
      socket.assigns
      |> Map.put(:socket, socket)
      |> Inspector.render()
      |> Phoenix.HTML.Safe.to_iodata()
      |> IO.iodata_to_binary()

    assert html =~ "Upkeep Inspector"
    assert html =~ ~s(id="upkeep-tabbed-workspace")
    assert html =~ ~s(id="upkeep-overview-tab")
    assert html =~ ~s(id="upkeep-flow-tab")
    assert html =~ ~s(id="upkeep-data-tab")
    assert html =~ ~s(id="upkeep-queries-tab")
    assert html =~ ~s(id="upkeep-invalidation-tab")
    assert html =~ ~s(id="upkeep-events-tab")
    assert html =~ ~s(id="upkeep-source-tab")
    assert html =~ ~s(id="upkeep-playground-panel")
    assert html =~ ~s(id="upkeep-node-inspector-panel")
    assert html =~ ~s(id="upkeep-timeline-panel")
    assert html =~ ~s(id="upkeep-optimization-panel")
    assert html =~ "Overview"
    assert html =~ "Assign Surface"
    assert html =~ "Sources &amp; Queries"
    assert html =~ "Invalidation"
    assert html =~ "Captured Source"
    assert html =~ "@issue_count is computed from @issues"
    assert html =~ "LiveView was not connected"
    assert html =~ "declared live"
    assert html =~ "no query dedup"
    assert html =~ "changed root"
    assert html =~ "recomputed, value changed"
    assert html =~ "upkeep.dag.recompute.stop"
    assert html =~ "graph_snapshot(%{"
    assert html =~ "Upkeep.watch(:issues"
    assert html =~ "Upkeep.derive(:issue_count"
    assert html =~ "changed bg-emerald-50"
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
end
