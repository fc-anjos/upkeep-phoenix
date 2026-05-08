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
    Enum.each(@default_values, fn {{source, id}, value} -> seed_source(source, id, value) end)
    Enum.each(@default_loads, fn {source, id} -> seed_load_counter(source, id) end)
    table
  end

  def reset! do
    if :ets.info(@table) != :undefined do
      :ets.delete(@table)
    end

    :ok
  end

  def set_source_value(source, id, value) do
    store({source, id}, value)
  end

  def seed_source(source, id, value) do
    set_source_value(source, id, value)
    seed_load_counter(source, id)
  end

  def seed_load_counter(source, id, count \\ 0) do
    store({:loads, source, id}, count)
  end

  def seed_load_counters(loads, id) do
    Enum.each(loads, &seed_load_counter(&1, id))
  end

  def load_source_value(source, id) do
    bump_load({:loads, source, id})
    fetch({source, id})
  end

  def bump_load(key) do
    :ets.update_counter(@table, key, 1)
  end

  def load_count(source, id \\ 1), do: fetch({:loads, source, id})

  def seed_scoped_issues(user_id, issues) do
    seed_source(:scoped_issues, user_id, issues)
  end

  def seed_scoped_activity(user_id, activity) do
    seed_source(:scoped_activity, user_id, activity)
  end

  def block_derives_for(user_id, test_pid) do
    store({:derive_test_pid, user_id}, test_pid)
  end

  def derive_blocker(user_id) do
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

  defp store(key, value) do
    :ets.insert(@table, {key, value})
  end

  defp fetch(key) do
    [{^key, value}] = :ets.lookup(@table, key)
    value
  end
end
