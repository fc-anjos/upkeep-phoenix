defmodule Upkeep.Internal.DAG.GraphTest do
  use ExUnit.Case, async: true

  alias Upkeep.Internal.DAG.Graph

  test "rejects cycles" do
    graph =
      Graph.new()
      |> Graph.put_node(:a, :source, [])
      |> Graph.put_node(:b, :derived, [:a])

    assert_raise ArgumentError, "cycle detected in Upkeep DAG", fn ->
      Graph.put_node(graph, :a, :source, [:b])
    end
  end

  test "rejects unknown dependencies" do
    assert_raise ArgumentError, ~r/unknown DAG dependencies/, fn ->
      Graph.put_node(Graph.new(), :a, :derived, [:missing])
    end
  end

  test "topological_order!, downstream_ids and affected_ids" do
    graph = sample_graph()

    assert Graph.topological_order!(graph) == [:issues, :visible, :count, :badge]
    assert Graph.downstream_ids(graph, :issues) == [:visible, :count, :badge]
    assert Graph.affected_ids(graph, [:visible]) == [:count, :badge]
  end

  test "subgraph_plan reports removable downstream nodes" do
    graph =
      Graph.new()
      |> Graph.put_node(:selected_issue_id, :source, [])
      |> Graph.put_node(:issue_detail, :component, [:selected_issue_id])
      |> Graph.put_node(:comments, :source, [:issue_detail])
      |> Graph.put_node(:comment_count, :derived, [:comments])

    plan = Graph.subgraph_plan(graph, :issue_detail)

    assert %Upkeep.Internal.DAG.Plan{} = plan
    assert plan.roots == [:issue_detail]
    assert plan.selected_node_ids == [:issue_detail, :comments, :comment_count]

    assert plan.subgraphs == [
             %{node_ids: [:issue_detail, :comments, :comment_count], count: 3}
           ]

    assert plan.largest_subgraphs == plan.subgraphs
    assert plan.boundaries == []
  end

  test "applicable_subgraphs reports largest upstream regions and boundaries" do
    graph =
      Graph.new()
      |> Graph.put_node(:stats, :source, [])
      |> Graph.put_node(:activity, :source, [])
      |> Graph.put_node(:project_name, :source, [])
      |> Graph.put_node(:current_scope, :source, [])
      |> Graph.put_node(:summary, :derived, [:stats, :activity])
      |> Graph.put_node(:dashboard, :derived, [:summary, :project_name, :current_scope])

    plan =
      Graph.applicable_subgraphs(graph, [:dashboard], fn
        id, _node when id in [:stats, :activity, :summary, :project_name] ->
          :include

        :current_scope, _node ->
          {:exclude, :current_scope}

        _id, _node ->
          {:exclude, :local_only_dep}
      end)

    assert %Upkeep.Internal.DAG.Plan{} = plan
    assert plan.roots == [:dashboard]
    assert plan.selected_node_ids == [:activity, :project_name, :stats, :summary]

    assert plan.subgraphs == [
             %{node_ids: [:activity, :stats, :summary], count: 3},
             %{node_ids: [:project_name], count: 1}
           ]

    assert plan.largest_subgraphs == [
             %{node_ids: [:activity, :stats, :summary], count: 3}
           ]

    assert plan.boundaries == [
             %{node_id: :current_scope, reason: :current_scope},
             %{node_id: :dashboard, reason: :local_only_dep}
           ]
  end

  test "snapshot exposes nodes, edges, and topological order" do
    snapshot = Graph.snapshot(sample_graph())
    nodes = Map.new(snapshot.nodes, fn node -> {node.id, node} end)

    assert snapshot.topological_order == [:issues, :visible, :count, :badge]

    assert snapshot.edges == [
             %{from: :issues, to: :visible},
             %{from: :visible, to: :count},
             %{from: :count, to: :badge}
           ]

    assert nodes.issues.kind == :source
    assert nodes.issues.dependents == [:visible]
    assert nodes.count.kind == :derived
    assert nodes.count.deps == [:visible]
  end

  defp sample_graph do
    Graph.new()
    |> Graph.put_node(:issues, :source, [])
    |> Graph.put_node(:visible, :derived, [:issues])
    |> Graph.put_node(:count, :derived, [:visible])
    |> Graph.put_node(:badge, :derived, [:count])
  end
end
