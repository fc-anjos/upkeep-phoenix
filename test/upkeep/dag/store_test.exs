defmodule Upkeep.Internal.DAG.StoreTest do
  use ExUnit.Case, async: true

  alias Upkeep.Internal.DAG.{Diff, Store}

  test "recomputes changed source descendants in topological order" do
    store =
      Store.new()
      |> elem_put_source(:issues, [:a])
      |> Store.put_derived(:visible, [:issues], fn %{issues: issues} -> issues end)
      |> Store.put_derived(:count, [:visible], fn %{visible: issues} -> length(issues) end)
      |> Store.put_derived(:badge, [:count], fn %{count: count} -> "Issues: #{count}" end)

    assert Store.fetch!(store, :count) == 1
    assert Store.fetch!(store, :badge) == "Issues: 1"

    {store, true} = Store.put_source(store, :issues, [:a, :b])
    {store, diff} = Store.recompute(store, [:issues])

    assert diff.changed_node_ids == [:visible, :count, :badge]
    assert diff.recomputed_node_ids == [:visible, :count, :badge]
    assert Store.fetch!(store, :count) == 2
    assert Store.fetch!(store, :badge) == "Issues: 2"
  end

  test "shared intermediate computations run once per refresh" do
    table = :ets.new(__MODULE__, [:set, :public])
    :ets.insert(table, {:visible_runs, 0})

    store =
      Store.new()
      |> elem_put_source(:issues, [:a])
      |> Store.put_derived(:visible, [:issues], fn %{issues: issues} ->
        :ets.update_counter(table, :visible_runs, 1)
        issues
      end)
      |> Store.put_derived(:count, [:visible], fn %{visible: issues} -> length(issues) end)
      |> Store.put_derived(:titles, [:visible], fn %{visible: issues} ->
        Enum.join(issues, ",")
      end)

    assert :ets.lookup_element(table, :visible_runs, 2) == 1

    {store, true} = Store.put_source(store, :issues, [:a, :b])
    {_store, diff} = Store.recompute(store, [:issues])

    assert diff.changed_node_ids == [:visible, :count, :titles]
    assert diff.recomputed_node_ids == [:visible, :count, :titles]
    assert :ets.lookup_element(table, :visible_runs, 2) == 2
  end

  test "unchanged intermediate values stop downstream recomputation" do
    table = :ets.new(__MODULE__, [:set, :public])
    :ets.insert(table, {:count_runs, 0})

    store =
      Store.new()
      |> elem_put_source(:issues, [:a])
      |> Store.put_derived(:visible, [:issues], fn %{issues: _issues} -> [:stable] end)
      |> Store.put_derived(:count, [:visible], fn %{visible: issues} ->
        :ets.update_counter(table, :count_runs, 1)
        length(issues)
      end)

    assert :ets.lookup_element(table, :count_runs, 2) == 1

    {store, true} = Store.put_source(store, :issues, [:a, :b])
    {_store, diff} = Store.recompute(store, [:issues])

    assert diff.changed_node_ids == []
    assert diff.recomputed_node_ids == [:visible]
    assert :ets.lookup_element(table, :count_runs, 2) == 1
  end

  test "recompute diff reports affected, recomputed, changed, and skipped nodes" do
    store =
      Store.new()
      |> elem_put_source(:issues, [:a])
      |> Store.put_derived(:visible, [:issues], fn %{issues: issues} -> issues end)
      |> Store.put_derived(:count, [:visible], fn %{visible: issues} -> length(issues) end)
      |> Store.put_derived(:badge, [:count], fn %{count: count} -> "Issues: #{count}" end)

    {store, true} = Store.put_source(store, :issues, [:a, :b])
    {_store, diff} = Store.recompute(store, [:issues], skip: [:count])

    assert %Diff{} = diff
    assert diff.roots == [:issues]
    assert diff.selected_node_ids == [:visible, :count, :badge]
    assert diff.recomputed_node_ids == [:visible]
    assert diff.changed_node_ids == [:visible]
    assert diff.skipped_node_ids == [:count]
    assert diff.boundaries == [%{node_id: :count, reason: :skipped}]
  end

  test "component nodes can own conditional source subgraphs" do
    store =
      Store.new()
      |> elem_put_source(:selected_issue_id, 1)
      |> Store.put_component(:issue_detail, [:selected_issue_id], fn %{selected_issue_id: id} ->
        %{issue_id: id}
      end)
      |> elem_put_source(:comments, [:first], [:issue_detail])
      |> Store.put_derived(:comment_count, [:comments], fn %{comments: comments} ->
        length(comments)
      end)

    assert Store.fetch!(store, :issue_detail) == %{issue_id: 1}
    assert Store.fetch!(store, :comment_count) == 1

    store = Store.remove_subgraph(store, :issue_detail)

    assert_raise KeyError, fn -> Store.fetch!(store, :issue_detail) end
    assert_raise KeyError, fn -> Store.fetch!(store, :comments) end
    assert_raise KeyError, fn -> Store.fetch!(store, :comment_count) end
    assert Store.fetch!(store, :selected_issue_id) == 1
  end

  test "owned source nodes are not recomputed as pure nodes" do
    store =
      Store.new()
      |> elem_put_source(:selected_issue_id, 1)
      |> Store.put_component(:issue_detail, [:selected_issue_id], fn %{selected_issue_id: id} ->
        %{issue_id: id}
      end)
      |> elem_put_source(:comments, [:first], [:issue_detail])
      |> Store.put_derived(:comment_count, [:comments], fn %{comments: comments} ->
        length(comments)
      end)

    {store, true} = Store.put_source(store, :selected_issue_id, 2)
    {store, diff} = Store.recompute(store, [:selected_issue_id])

    assert diff.changed_node_ids == [:issue_detail]
    assert diff.recomputed_node_ids == [:issue_detail]
    assert Store.fetch!(store, :issue_detail) == %{issue_id: 2}
    assert Store.fetch!(store, :comments) == [:first]
    assert Store.fetch!(store, :comment_count) == 1
  end

  test "register_derived defers compute until first recompute touches the node" do
    store =
      Store.new()
      |> elem_put_source(:a, nil)
      |> Store.register_derived(:b, [:a], fn %{a: a} -> a + 1 end)

    assert_raise KeyError, fn -> Store.fetch!(store, :b) end

    {store, true} = Store.put_source(store, :a, 1)
    {store, diff} = Store.recompute(store, [:a])

    assert diff.changed_node_ids == [:b]
    assert Store.fetch!(store, :b) == 2
  end

  test "seed sets a value without recomputing downstream" do
    store =
      Store.new()
      |> elem_put_source(:a, 1)
      |> Store.put_derived(:b, [:a], fn %{a: a} -> a * 10 end)

    {store, true} = Store.seed(store, :b, 999)
    assert Store.fetch!(store, :b) == 999

    assert_raise ArgumentError, ~r/unknown DAG node/, fn ->
      Store.seed(store, :ghost, :x)
    end
  end

  test "metadata: put + fetch + update + drop on remove_subgraph" do
    store =
      Store.new()
      |> elem_put_source(:a, 1)
      |> Store.put_metadata(:a, %{loader: :fn_a, hits: 0})

    assert Store.fetch_metadata!(store, :a) == %{loader: :fn_a, hits: 0}
    assert Store.get_metadata(store, :missing) == nil
    assert Store.get_metadata(store, :missing, :default) == :default

    store =
      Store.update_metadata(store, :a, %{hits: 0}, fn meta ->
        Map.update!(meta, :hits, &(&1 + 1))
      end)

    assert Store.fetch_metadata!(store, :a) == %{loader: :fn_a, hits: 1}

    store = Store.remove_subgraph(store, :a)
    assert Store.get_metadata(store, :a) == nil
  end

  test "metadata: raises for unknown nodes" do
    store = Store.new()

    assert_raise ArgumentError, ~r/unknown DAG node/, fn ->
      Store.put_metadata(store, :ghost, %{})
    end

    assert_raise ArgumentError, ~r/unknown DAG node/, fn ->
      Store.update_metadata(store, :ghost, %{}, & &1)
    end
  end

  defp elem_put_source(store, id, value, deps \\ []) do
    {store, _changed?} = Store.put_source(store, id, value, deps)
    store
  end
end
