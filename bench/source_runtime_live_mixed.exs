# LiveView-level mixed source runtime benchmark.
#
#   mix run bench/source_runtime_live_mixed.exs
#
# Exercises the public Live.watch/derive path with:
#
#   - shared project sources reused by many LiveViews,
#   - one-off per-user sources mounted in the same LiveViews,
#   - local derived assigns applied through Live.apply_dag_values/2,
#   - steady-state updates delivered through the coordinator graph.

Code.require_file("support/initial_sharing.exs", __DIR__)

defmodule Bench.SourceRuntimeLiveMixed.ProjectEvent do
  defstruct [:project_id]
end

defmodule Bench.SourceRuntimeLiveMixed.UserPrefsEvent do
  defstruct [:user_id]
end

defmodule Bench.SourceRuntimeLiveMixed.Items do
  use Upkeep.Source

  def load(params) do
    Bench.SourceRuntimeLiveMixed.load(:items, params.project_id)
  end

  invalidated_by(Bench.SourceRuntimeLiveMixed.ProjectEvent, on: :project_id)
end

defmodule Bench.SourceRuntimeLiveMixed.Activity do
  use Upkeep.Source

  def load(params) do
    Bench.SourceRuntimeLiveMixed.load(:activity, params.project_id)
  end

  invalidated_by(Bench.SourceRuntimeLiveMixed.ProjectEvent, on: :project_id)
end

defmodule Bench.SourceRuntimeLiveMixed.UserPrefs do
  use Upkeep.Source

  def load(params) do
    Bench.SourceRuntimeLiveMixed.load(:prefs, params.user_id)
  end

  invalidated_by(Bench.SourceRuntimeLiveMixed.UserPrefsEvent, on: :user_id)
end

defmodule Bench.SourceRuntimeLiveMixed.Compute do
  def item_count(%{items: items}), do: length(items)
  def items_version(%{items: [%{version: version} | _items]}), do: version
  def items_version(%{items: []}), do: nil

  def dashboard_score(%{item_count: item_count, activity: activity, prefs: prefs}) do
    item_count + length(activity) + prefs.weight
  end
end

defmodule Bench.SourceRuntimeLiveMixed do
  alias Bench.SourceRuntimeLiveMixed.{Activity, Compute, Items, ProjectEvent, UserPrefs}
  alias Upkeep.Live

  @table Bench.SourceRuntimeLiveMixed.Table
  @await_timeout_ms 30_000

  def run do
    subscribers = env_int("BENCH_SUBSCRIBERS", 400)
    projects = env_int("BENCH_PROJECTS", 20)
    iterations = env_int("BENCH_ITERATIONS", 20)
    item_count = env_int("BENCH_ITEMS", 100)

    result = run_once(subscribers, projects, iterations, item_count)

    print_report(subscribers, projects, iterations, item_count, result)
    validate!(result, subscribers, iterations, projects)
  end

  def load(kind, id) do
    :ets.update_counter(@table, {:loads, kind}, {2, 1}, {{:loads, kind}, 0})
    :ets.lookup_element(@table, {kind, id}, 2)
  end

  defp run_once(subscribers, projects, iterations, item_count) do
    Upkeep.Coordinator.Graph.reset()
    ensure_table()

    try do
      seed_projects(projects, item_count)
      parent = self()

      pids =
        for idx <- 1..subscribers do
          project_id = rem(idx - 1, projects) + 1
          user_id = {:user, idx}
          seed_user(user_id)
          spawn_link(fn -> subscriber(parent, project_id, user_id) end)
        end

      wait_for_ready(subscribers)
      reset_load_counters()

      latencies =
        for iteration <- 1..iterations do
          project_id = rem(iteration - 1, projects) + 1
          expected_updates = subscribers_for_project(subscribers, projects, project_id)
          update_project(project_id, item_count, iteration)

          drain_values()
          started_at = System.monotonic_time(:microsecond)

          :ok = Upkeep.Invalidation.dispatch(%ProjectEvent{project_id: project_id})
          :ok = Upkeep.Coordinator.Graph.drain()
          wait_for_updates(iteration, expected_updates)

          System.monotonic_time(:microsecond) - started_at
        end

      Enum.each(pids, &send(&1, :stop))
      wait_for_stopped(subscribers)

      %{
        latencies: latencies,
        items_loads: load_count(:items),
        activity_loads: load_count(:activity),
        prefs_loads: load_count(:prefs)
      }
    after
      Upkeep.Coordinator.Graph.reset()
    end
  end

  defp subscriber(parent, project_id, user_id) do
    socket =
      Bench.InitialSharingSupport.socket()
      |> Live.watch(:items, Items, project_id: project_id)
      |> Live.watch(:activity, Activity, project_id: project_id)
      |> Live.watch(:prefs, UserPrefs, user_id: user_id)
      |> Live.derive(:item_count, [:items], &Compute.item_count/1)
      |> Live.derive(:items_version, [:items], &Compute.items_version/1)
      |> Live.derive(:dashboard_score, [:item_count, :activity, :prefs], &Compute.dashboard_score/1)

    send(parent, {:ready, self()})
    loop(parent, socket)
  end

  defp loop(parent, socket) do
    receive do
      {:dag_values, pairs} ->
        socket = Live.apply_dag_values(socket, pairs)
        send(parent, {:updated, self(), socket.assigns.items_version})
        loop(parent, socket)

      :stop ->
        Enum.each(Map.keys(socket.private.upkeep_runtime.watches), &Upkeep.Coordinator.Graph.unregister/1)
        send(parent, {:stopped, self()})
    after
      @await_timeout_ms ->
        raise "subscriber timed out"
    end
  end

  defp ensure_table do
    case :ets.info(@table) do
      :undefined -> :ets.new(@table, [:set, :public, :named_table])
      _ -> :ets.delete_all_objects(@table)
    end
  end

  defp seed_projects(projects, item_count) do
    Enum.each(1..projects, fn project_id ->
      insert_project(project_id, item_count, 0)
    end)
  end

  defp update_project(project_id, item_count, version) do
    insert_project(project_id, item_count, version)
  end

  defp insert_project(project_id, item_count, version) do
    items =
      for idx <- 1..item_count do
        %{id: {project_id, idx}, version: version}
      end

    activity =
      for idx <- 1..10 do
        %{id: {project_id, idx}, version: version}
      end

    :ets.insert(@table, {{:items, project_id}, items})
    :ets.insert(@table, {{:activity, project_id}, activity})
  end

  defp seed_user(user_id) do
    :ets.insert(@table, {{:prefs, user_id}, %{weight: 7}})
  end

  defp reset_load_counters do
    Enum.each([:items, :activity, :prefs], fn kind ->
      :ets.insert(@table, {{:loads, kind}, 0})
    end)
  end

  defp load_count(kind) do
    :ets.lookup_element(@table, {:loads, kind}, 2)
  end

  defp wait_for_ready(count) do
    for _ <- 1..count do
      receive do
        {:ready, _pid} -> :ok
      after
        @await_timeout_ms -> raise "subscribers did not finish mounting"
      end
    end
  end

  defp wait_for_updates(version, count) do
    for _ <- 1..count do
      receive do
        {:updated, _pid, ^version} -> :ok
      after
        @await_timeout_ms -> raise "subscribers did not receive update #{version}"
      end
    end
  end

  defp wait_for_stopped(count) do
    for _ <- 1..count do
      receive do
        {:stopped, _pid} -> :ok
      after
        @await_timeout_ms -> raise "subscribers did not stop"
      end
    end
  end

  defp drain_values do
    receive do
      {:dag_values, _pairs} -> drain_values()
      {:updated, _pid, _version} -> drain_values()
    after
      0 -> :ok
    end
  end

  defp subscribers_for_project(subscribers, projects, project_id) do
    1..subscribers
    |> Enum.count(fn idx -> rem(idx - 1, projects) + 1 == project_id end)
  end

  defp print_report(subscribers, projects, iterations, item_count, result) do
    IO.puts(
      "source_runtime_live_mixed subscribers=#{subscribers} projects=#{projects} " <>
        "iterations=#{iterations} items=#{item_count}"
    )

    IO.puts("")
    IO.puts(format_row(["runtime", "items", "activity", "prefs", "p50_us", "p95_us", "p99_us"]))

    IO.puts(
      format_row([
        "source_process",
        Integer.to_string(result.items_loads),
        Integer.to_string(result.activity_loads),
        Integer.to_string(result.prefs_loads),
        Integer.to_string(percentile(result.latencies, 0.50)),
        Integer.to_string(percentile(result.latencies, 0.95)),
        Integer.to_string(percentile(result.latencies, 0.99))
      ])
    )
  end

  defp validate!(result, _subscribers, iterations, _projects) do
    unless result.items_loads <= iterations and result.activity_loads <= iterations and
             result.prefs_loads == 0 do
      raise "loaded shared project sources too many times: " <>
              "#{result.items_loads}/#{result.activity_loads}/#{result.prefs_loads}"
    end

    IO.puts("\nOK")
  end

  defp percentile(values, percentile) do
    values = Enum.sort(values)
    index = max(0, ceil(length(values) * percentile) - 1)
    Enum.at(values, index)
  end

  defp format_row(values) do
    values
    |> Enum.with_index()
    |> Enum.map_join(" ", fn {value, index} ->
      String.pad_trailing(value, column_width(index))
    end)
  end

  defp column_width(0), do: 16
  defp column_width(_index), do: 10

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

end

Bench.SourceRuntimeLiveMixed.run()
