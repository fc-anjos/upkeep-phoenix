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

  defp elem_put_source(dag, id, value) do
    {dag, _changed?} = DAG.put_source(dag, id, value)
    dag
  end
end
