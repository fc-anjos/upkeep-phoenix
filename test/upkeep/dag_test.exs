defmodule Upkeep.DAGTest do
  use ExUnit.Case, async: true

  alias Upkeep.DAG

  test "recomputes changed source descendants in topological order" do
    dag =
      DAG.new()
      |> elem_put_source(:issues, [:a])
      |> DAG.put_derived(:visible, [:issues], fn %{issues: issues} -> issues end)
      |> DAG.put_derived(:count, [:visible], fn %{visible: issues} -> length(issues) end)
      |> DAG.put_derived(:badge, [:count], fn %{count: count} -> "Issues: #{count}" end)

    assert DAG.fetch!(dag, :count) == 1
    assert DAG.fetch!(dag, :badge) == "Issues: 1"

    {dag, true} = DAG.put_source(dag, :issues, [:a, :b])
    {dag, changed, recomputed} = DAG.recompute(dag, [:issues])

    assert changed == [:visible, :count, :badge]
    assert recomputed == [:visible, :count, :badge]
    assert DAG.fetch!(dag, :count) == 2
    assert DAG.fetch!(dag, :badge) == "Issues: 2"
  end

  test "shared intermediate computations run once per refresh" do
    table = :ets.new(__MODULE__, [:set, :public])
    :ets.insert(table, {:visible_runs, 0})

    dag =
      DAG.new()
      |> elem_put_source(:issues, [:a])
      |> DAG.put_derived(:visible, [:issues], fn %{issues: issues} ->
        :ets.update_counter(table, :visible_runs, 1)
        issues
      end)
      |> DAG.put_derived(:count, [:visible], fn %{visible: issues} -> length(issues) end)
      |> DAG.put_derived(:titles, [:visible], fn %{visible: issues} -> Enum.join(issues, ",") end)

    assert :ets.lookup_element(table, :visible_runs, 2) == 1

    {dag, true} = DAG.put_source(dag, :issues, [:a, :b])
    {_dag, changed, recomputed} = DAG.recompute(dag, [:issues])

    assert changed == [:visible, :count, :titles]
    assert recomputed == [:visible, :count, :titles]
    assert :ets.lookup_element(table, :visible_runs, 2) == 2
  end

  test "unchanged intermediate values stop downstream recomputation" do
    table = :ets.new(__MODULE__, [:set, :public])
    :ets.insert(table, {:count_runs, 0})

    dag =
      DAG.new()
      |> elem_put_source(:issues, [:a])
      |> DAG.put_derived(:visible, [:issues], fn %{issues: _issues} -> [:stable] end)
      |> DAG.put_derived(:count, [:visible], fn %{visible: issues} ->
        :ets.update_counter(table, :count_runs, 1)
        length(issues)
      end)

    assert :ets.lookup_element(table, :count_runs, 2) == 1

    {dag, true} = DAG.put_source(dag, :issues, [:a, :b])
    {_dag, changed, recomputed} = DAG.recompute(dag, [:issues])

    assert changed == []
    assert recomputed == [:visible]
    assert :ets.lookup_element(table, :count_runs, 2) == 1
  end

  test "rejects cycles" do
    dag =
      DAG.new()
      |> elem_put_source(:a, 1)
      |> DAG.put_derived(:b, [:a], fn %{a: a} -> a + 1 end)

    assert_raise ArgumentError, "cycle detected in Upkeep DAG", fn ->
      DAG.put_derived(dag, :a, [:b], fn %{b: b} -> b + 1 end)
    end
  end

  test "component nodes can own conditional source subgraphs" do
    dag =
      DAG.new()
      |> elem_put_source(:selected_issue_id, 1)
      |> DAG.put_component(:issue_detail, [:selected_issue_id], fn %{selected_issue_id: id} ->
        %{issue_id: id}
      end)
      |> elem_put_source(:comments, [:first], [:issue_detail])
      |> DAG.put_derived(:comment_count, [:comments], fn %{comments: comments} ->
        length(comments)
      end)

    assert DAG.fetch!(dag, :issue_detail) == %{issue_id: 1}
    assert DAG.fetch!(dag, :comment_count) == 1
    assert DAG.downstream_ids(dag, :issue_detail) == [:comments, :comment_count]

    dag = DAG.remove_subgraph(dag, :issue_detail)

    assert_raise KeyError, fn -> DAG.fetch!(dag, :issue_detail) end
    assert_raise KeyError, fn -> DAG.fetch!(dag, :comments) end
    assert_raise KeyError, fn -> DAG.fetch!(dag, :comment_count) end
    assert DAG.fetch!(dag, :selected_issue_id) == 1
  end

  test "owned source nodes are not recomputed as pure nodes" do
    dag =
      DAG.new()
      |> elem_put_source(:selected_issue_id, 1)
      |> DAG.put_component(:issue_detail, [:selected_issue_id], fn %{selected_issue_id: id} ->
        %{issue_id: id}
      end)
      |> elem_put_source(:comments, [:first], [:issue_detail])
      |> DAG.put_derived(:comment_count, [:comments], fn %{comments: comments} ->
        length(comments)
      end)

    {dag, true} = DAG.put_source(dag, :selected_issue_id, 2)
    {dag, changed, recomputed} = DAG.recompute(dag, [:selected_issue_id])

    assert changed == [:issue_detail]
    assert recomputed == [:issue_detail]
    assert DAG.fetch!(dag, :issue_detail) == %{issue_id: 2}
    assert DAG.fetch!(dag, :comments) == [:first]
    assert DAG.fetch!(dag, :comment_count) == 1
  end

  test "snapshot exposes nodes, edges, and affected order" do
    dag =
      DAG.new()
      |> elem_put_source(:issues, [:a])
      |> DAG.put_derived(:visible, [:issues], fn %{issues: issues} -> issues end)
      |> DAG.put_derived(:count, [:visible], fn %{visible: issues} -> length(issues) end)
      |> DAG.put_derived(:badge, [:count], fn %{count: count} -> "Issues: #{count}" end)

    snapshot = DAG.snapshot(dag)
    nodes = Map.new(snapshot.nodes, fn node -> {node.id, node} end)

    assert snapshot.topological_order == [:issues, :visible, :count, :badge]

    assert snapshot.edges == [
             %{from: :issues, to: :visible},
             %{from: :visible, to: :count},
             %{from: :count, to: :badge}
           ]

    assert nodes.issues.kind == :source
    assert nodes.issues.dependents == [:visible]
    assert nodes.issues.value == [:a]

    assert nodes.count.kind == :derived
    assert nodes.count.deps == [:visible]
    assert nodes.count.value == 1

    assert DAG.affected_ids(dag, [:issues]) == [:visible, :count, :badge]
  end

  defp elem_put_source(dag, id, value, deps \\ []) do
    {dag, _changed?} = DAG.put_source(dag, id, value, deps)
    dag
  end
end
