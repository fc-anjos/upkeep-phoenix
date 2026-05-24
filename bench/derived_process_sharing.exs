# Derived process sharing benchmark.
#
#   mix run bench/derived_process_sharing.exs
#
# Compares the existing Live.derive/4 API in two shapes:
#
#   - shareable external function derives, materialized once per dependency identity,
#   - captured function derives, kept local to each LiveView.

Code.require_file("support/initial_sharing.exs", __DIR__)

defmodule Bench.DerivedProcessSharing.ProjectEvent do
  defstruct [:project_id]
end

defmodule Bench.DerivedProcessSharing.Items do
  use Upkeep.Source

  def load(params) do
    Bench.DerivedProcessSharing.load_items(params.project_id)
  end

  invalidated_by(Bench.DerivedProcessSharing.ProjectEvent, on: :project_id)
end

defmodule Bench.DerivedProcessSharing.Compute do
  def shared_item_summary(%{items: items}) do
    Bench.DerivedProcessSharing.item_summary(:shared, items)
  end

  def local_item_summary(items) do
    Bench.DerivedProcessSharing.item_summary(:local, items)
  end
end

defmodule Bench.DerivedProcessSharing do
  alias Bench.DerivedProcessSharing.{Compute, Items, ProjectEvent}
  alias Upkeep.Live

  @table Bench.DerivedProcessSharing.Table
  @await_timeout_ms 30_000

  def run do
    subscribers = env_int("BENCH_SUBSCRIBERS", 400)
    projects = env_int("BENCH_PROJECTS", 20)
    iterations = env_int("BENCH_ITERATIONS", 20)
    item_count = env_int("BENCH_ITEMS", 100)
    compute_us = env_int("BENCH_COMPUTE_US", 0)

    shared = run_case(:shared, subscribers, projects, iterations, item_count, compute_us)
    local = run_case(:local, subscribers, projects, iterations, item_count, compute_us)

    print_report(subscribers, projects, iterations, item_count, compute_us, [shared, local])
    validate!(shared, local, subscribers, projects, iterations)
  end

  def load_items(project_id) do
    :ets.update_counter(@table, {:loads, :items}, {2, 1}, {{:loads, :items}, 0})
    :ets.lookup_element(@table, {:items, project_id}, 2)
  end

  def item_summary(kind, [%{project_id: project_id, version: version} | _items] = items) do
    :ets.update_counter(@table, {:computes, kind}, {2, 1}, {{:computes, kind}, 0})
    burn_compute_time()
    {project_id, length(items), version}
  end

  def item_summary(kind, []) do
    :ets.update_counter(@table, {:computes, kind}, {2, 1}, {{:computes, kind}, 0})
    burn_compute_time()
    {:empty, 0, nil}
  end

  defp run_case(kind, subscribers, projects, iterations, item_count, compute_us) do
    Upkeep.Coordinator.Graph.reset()
    Bench.InitialSharingSupport.ensure_table(@table)
    :ets.insert(@table, {:compute_us, compute_us})

    try do
      seed_projects(projects, item_count, 0)
      reset_counters()
      parent = self()

      {mount_us, pids} =
        :timer.tc(fn ->
          pids =
            for idx <- 1..subscribers do
              project_id = rem(idx - 1, projects) + 1
              spawn_link(fn -> subscriber(parent, kind, project_id) end)
            end

          wait_for_ready(subscribers)
          pids
        end)

      initial_computes = compute_count(kind)
      reset_counters()

      latencies =
        for iteration <- 1..iterations do
          project_id = rem(iteration - 1, projects) + 1
          expected_updates = subscribers_for_project(subscribers, projects, project_id)
          seed_project(project_id, item_count, iteration)

          drain_parent_messages()
          started_at = System.monotonic_time(:microsecond)

          :ok = Upkeep.Invalidation.dispatch(%ProjectEvent{project_id: project_id})
          :ok = Upkeep.Coordinator.Graph.drain()
          wait_for_updates(project_id, item_count, iteration, expected_updates)

          System.monotonic_time(:microsecond) - started_at
        end

      Enum.each(pids, &send(&1, :stop))
      wait_for_stopped(subscribers)

      %{
        case: kind,
        mount_us: mount_us,
        initial_computes: initial_computes,
        refresh_computes: compute_count(kind),
        source_loads: load_count(:items),
        source_processes: source_process_count(),
        derived_processes: derived_process_count(),
        latencies: latencies
      }
    after
      Upkeep.Coordinator.Graph.reset()
    end
  end

  defp subscriber(parent, :shared, project_id) do
    socket =
      Bench.InitialSharingSupport.socket()
      |> Live.watch(:items, Items, project_id: project_id)
      |> Live.derive(:item_summary, [:items], &Compute.shared_item_summary/1)

    send(parent, {:ready, self()})
    loop(parent, socket)
  end

  defp subscriber(parent, :local, project_id) do
    kind = :local

    socket =
      Bench.InitialSharingSupport.socket()
      |> Live.watch(:items, Items, project_id: project_id)
      |> Live.derive(:item_summary, [:items], fn %{items: items} ->
        Compute.local_item_summary(items) || kind
      end)

    send(parent, {:ready, self()})
    loop(parent, socket)
  end

  defp loop(parent, socket) do
    receive do
      {:dag_values, pairs} ->
        socket = Live.apply_dag_values(socket, pairs)
        send(parent, {:updated, self(), socket.assigns.item_summary})
        loop(parent, socket)

      :stop ->
        Enum.each(
          Map.keys(socket.private.upkeep_runtime.watches),
          &Upkeep.Coordinator.Graph.unregister/1
        )

        send(parent, {:stopped, self()})
    after
      @await_timeout_ms ->
        raise "subscriber timed out"
    end
  end

  defp seed_projects(projects, item_count, version) do
    Enum.each(1..projects, &seed_project(&1, item_count, version))
  end

  defp seed_project(project_id, item_count, version) do
    items =
      for idx <- 1..item_count do
        %{id: {project_id, idx}, project_id: project_id, version: version}
      end

    :ets.insert(@table, {{:items, project_id}, items})
  end

  defp reset_counters do
    Enum.each([{:loads, :items}, {:computes, :shared}, {:computes, :local}], fn key ->
      :ets.insert(@table, {key, 0})
    end)
  end

  defp load_count(kind), do: :ets.lookup_element(@table, {:loads, kind}, 2)
  defp compute_count(kind), do: :ets.lookup_element(@table, {:computes, kind}, 2)

  defp wait_for_ready(count) do
    for _ <- 1..count do
      receive do
        {:ready, _pid} -> :ok
      after
        @await_timeout_ms -> raise "subscribers did not finish mounting"
      end
    end
  end

  defp wait_for_updates(project_id, item_count, version, count) do
    expected = {project_id, item_count, version}

    for _ <- 1..count do
      receive do
        {:updated, _pid, ^expected} -> :ok
      after
        @await_timeout_ms ->
          raise "subscribers did not receive derived update #{inspect(expected)}"
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

  defp drain_parent_messages do
    receive do
      {:updated, _pid, _summary} -> drain_parent_messages()
    after
      0 -> :ok
    end
  end

  defp subscribers_for_project(subscribers, projects, project_id) do
    1..subscribers
    |> Enum.count(fn idx -> rem(idx - 1, projects) + 1 == project_id end)
  end

  defp print_report(subscribers, projects, iterations, item_count, compute_us, results) do
    IO.puts(
      "derived_process_sharing subscribers=#{subscribers} projects=#{projects} " <>
        "iterations=#{iterations} items=#{item_count} compute_us=#{compute_us}"
    )

    IO.puts("")

    IO.puts(
      format_row([
        "case",
        "init_cmp",
        "refresh_cmp",
        "loads",
        "src_procs",
        "drv_procs",
        "mount_ms",
        "p50_us",
        "p95_us",
        "p99_us"
      ])
    )

    Enum.each(results, fn result ->
      IO.puts(
        format_row([
          Atom.to_string(result.case),
          Integer.to_string(result.initial_computes),
          Integer.to_string(result.refresh_computes),
          Integer.to_string(result.source_loads),
          Integer.to_string(result.source_processes),
          Integer.to_string(result.derived_processes),
          :erlang.float_to_binary(result.mount_us / 1_000, decimals: 2),
          Integer.to_string(percentile(result.latencies, 0.50)),
          Integer.to_string(percentile(result.latencies, 0.95)),
          Integer.to_string(percentile(result.latencies, 0.99))
        ])
      )
    end)
  end

  defp validate!(shared, local, subscribers, projects, iterations) do
    expected_shared_initial = min(subscribers, projects)
    expected_shared_refresh = iterations

    unless shared.initial_computes == expected_shared_initial and
             shared.refresh_computes == expected_shared_refresh do
      raise "shared derive compute count was #{shared.initial_computes}/#{shared.refresh_computes}, " <>
              "expected #{expected_shared_initial}/#{expected_shared_refresh}"
    end

    expected_local_refresh =
      1..iterations
      |> Enum.map(fn iteration -> rem(iteration - 1, projects) + 1 end)
      |> Enum.map(&subscribers_for_project(subscribers, projects, &1))
      |> Enum.sum()

    unless local.initial_computes == subscribers and
             local.refresh_computes == expected_local_refresh do
      raise "local derive compute count was #{local.initial_computes}/#{local.refresh_computes}, " <>
              "expected #{subscribers}/#{expected_local_refresh}"
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

  defp column_width(0), do: 12
  defp column_width(_index), do: 12

  defp source_process_count do
    case Process.whereis(Upkeep.Coordinator.SourceProcesses.Registry) do
      nil -> 0
      _pid -> Upkeep.Coordinator.SourceProcesses.count()
    end
  end

  defp derived_process_count do
    case Process.whereis(Upkeep.Coordinator.DerivedProcesses.Registry) do
      nil -> 0
      _pid -> Upkeep.Coordinator.DerivedProcesses.count()
    end
  end

  defp burn_compute_time do
    case :ets.lookup(@table, :compute_us) do
      [{:compute_us, compute_us}] when compute_us > 0 ->
        deadline = System.monotonic_time(:microsecond) + compute_us
        spin_until(deadline)

      _other ->
        :ok
    end
  end

  defp spin_until(deadline) do
    if System.monotonic_time(:microsecond) < deadline do
      spin_until(deadline)
    else
      :ok
    end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

Bench.DerivedProcessSharing.run()
