defmodule Upkeep.TestSupport.LiveRefreshFixture do
  @moduledoc false

  @table __MODULE__

  @default_values [
    {{:issues, 1}, [:issue_a]},
    {{:activity, 1}, [:activity_a]},
    {{:failing, 1}, [:stable]},
    {{:comments, 1}, [:comment_a]},
    {{:comments, 2}, [:comment_b]},
    {{:scoped_issues, 1}, [:user_1_issue]},
    {{:scoped_issues, 2}, [:user_2_issue]}
  ]

  @default_loads [
    {:issues, 1},
    {:activity, 1},
    {:failing, 1},
    {:comments, 1},
    {:comments, 2},
    {:scoped_issues, 1},
    {:scoped_issues, 2},
    {:scoped_activity, 1},
    {:scoped_activity, 2},
    {:visible, 1},
    {:issue_count, 1},
    {:issue_label, 1}
  ]

  def setup! do
    reset!()
    table = :ets.new(@table, [:set, :public, :named_table])
    Enum.each(@default_values, fn {key, value} -> put(key, value) end)
    Enum.each(@default_loads, fn {source, id} -> put_load(source, id) end)
    table
  end

  def reset! do
    if :ets.info(@table) != :undefined do
      :ets.delete(@table)
    end

    :ok
  end

  def put(key, value) do
    :ets.insert(@table, {key, value})
  end

  def put_value(source, id, value) do
    put({source, id}, value)
  end

  def put_loaded_value(source, id, value) do
    put_value(source, id, value)
    put_load(source, id)
  end

  def put_load(source, id, count \\ 0) do
    put({:loads, source, id}, count)
  end

  def value(key) do
    [{^key, value}] = :ets.lookup(@table, key)
    value
  end

  def load_value(source, id) do
    bump_load({:loads, source, id})
    value({source, id})
  end

  def bump_load(key) do
    :ets.update_counter(@table, key, 1)
  end

  def load_count(source, id \\ 1), do: value({:loads, source, id})

  def put_scoped_user(user_id, issues) do
    put_loaded_value(:scoped_issues, user_id, issues)
  end

  def put_scoped_activity(user_id, activity) do
    put_loaded_value(:scoped_activity, user_id, activity)
  end

  def put_derive_test_pid(user_id, test_pid) do
    put({:derive_test_pid, user_id}, test_pid)
  end

  def derive_test_pid(user_id) do
    case :ets.lookup(@table, {:derive_test_pid, user_id}) do
      [{_, test_pid}] -> {:ok, test_pid}
      [] -> :error
    end
  end

  def project_id_for_issues(issues) do
    case :ets.match(@table, {{:issues, :"$1"}, issues}) do
      [[id] | _] -> {:ok, id}
      [] -> :error
    end
  end
end
