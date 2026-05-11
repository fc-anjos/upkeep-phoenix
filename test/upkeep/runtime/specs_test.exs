defmodule Upkeep.Runtime.SpecsTest do
  use ExUnit.Case, async: true

  alias Upkeep.Runtime
  alias Upkeep.Runtime.Materializer
  alias Upkeep.Runtime.Producer
  alias Upkeep.Runtime.Specs
  alias Upkeep.Source.Instance
  alias Upkeep.TestSupport.LiveSocket

  defmodule Issue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    def load(_params), do: []

    invalidated_by(Issue, :updated, on: :project_id)
  end

  defmodule Calculations do
    def count(%{issues: issues}), do: length(issues)
  end

  test "builds source node specs from watch inputs" do
    spec = Specs.source(LiveSocket.socket(), :issues, ProjectIssues, %{project_id: 1}, nil)

    source_id = {ProjectIssues, %{project_id: 1}}
    instance = Instance.build(ProjectIssues, %{project_id: 1})

    assert spec.id == {:source, source_id}
    assert spec.kind == :source
    assert spec.deps == []
    assert spec.scope == :shared

    assert spec.producer == %Producer.Source{
             instance: instance,
             source_id: source_id,
             component: nil
           }

    assert spec.materializers == [
             %Materializer.Assign{
               assign_name: :issues,
               node_id: {:source, source_id},
               kind: :source
             }
           ]

    assert spec.metadata.assign_name == :issues
    assert spec.metadata.source_id == source_id
  end

  test "builds component node specs from component inputs" do
    socket = socket_with_issues()

    spec =
      Specs.component(socket, {:issue_panel, 1}, [:issues], fn %{issues: issues} -> issues end)

    source_id = {ProjectIssues, %{project_id: 1}}

    assert spec.id == {:component, {:issue_panel, 1}}
    assert spec.kind == :component
    assert spec.deps == [{:source, source_id}]
    assert spec.scope == :local

    assert %Producer.Compute{
             deps: [{:source, ^source_id}],
             dep_pairs: [issues: {:source, ^source_id}]
           } =
             spec.producer

    assert spec.materializers == [
             %Materializer.Component{
               component_id: {:issue_panel, 1},
               node_id: {:component, {:issue_panel, 1}}
             }
           ]
  end

  test "builds derived node specs from derive inputs" do
    socket = socket_with_issues()
    spec = Specs.derived(socket, :issue_count, [:issues], &Calculations.count/1)
    source_id = {ProjectIssues, %{project_id: 1}}

    assert spec.id == {:derived, :issue_count}
    assert spec.kind == :derived
    assert spec.deps == [{:source, source_id}]
    assert spec.scope == :local_or_shared

    assert %Producer.Compute{
             deps: [{:source, ^source_id}],
             dep_pairs: [issues: {:source, ^source_id}],
             identity: {Calculations, :count, 1}
           } = spec.producer

    assert spec.materializers == [
             %Materializer.Assign{
               assign_name: :issue_count,
               node_id: {:derived, :issue_count},
               kind: :derived
             }
           ]
  end

  defp socket_with_issues do
    assert {:ok, socket, _effects} =
             Runtime.mount_source(
               LiveSocket.socket(),
               :issues,
               ProjectIssues,
               %{project_id: 1},
               nil
             )

    socket
  end
end
