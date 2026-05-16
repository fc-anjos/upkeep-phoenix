# Source process idle retention benchmark.
#
#   mix run bench/source_runtime_idle_retention.exs
#
# Compares process-runtime shared-source attach/detach cycles with immediate
# stop versus structural idle retention. Every source has two concurrent
# subscribers, so it is eligible for retention after becoming idle.

Application.ensure_all_started(:upkeep)

defmodule Bench.SourceRuntimeIdleRetention.Event do
  defstruct [:scope]
end

defmodule Bench.SourceRuntimeIdleRetention do
  alias Bench.SourceRuntimeIdleRetention.Event
  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.SourceProcesses
  alias Upkeep.InvalidationSurface

  @await_timeout_ms 30_000

  def run do
    source_count = env_int("BENCH_RETAINED_SOURCES", env_int("BENCH_SOURCES", 250))
    cycles = env_int("BENCH_CYCLES", env_int("BENCH_ITERATIONS", 5))

    rows =
      Enum.map([0, :infinity], fn ttl ->
        {ttl, run_policy(ttl, source_count, cycles)}
      end)

    print_report(source_count, cycles, rows)
    validate!(source_count, cycles, rows)
  end

  defp run_policy(ttl, source_count, cycles) do
    old_ttl = Application.get_env(:upkeep, :source_idle_ttl_ms)

    Application.put_env(:upkeep, :source_idle_ttl_ms, ttl)
    Graph.reset()

    try do
      parent = self()
      event = %Event{scope: :all}
      surface = surface()
      loads = :counters.new(1, [:atomics])

      source_ids =
        for idx <- 1..source_count do
          {:bench_source_runtime_idle_retention, ttl, idx}
        end

      latencies =
        for cycle <- 1..cycles do
          drain_values()
          started_at = System.monotonic_time(:microsecond)

          children =
            Enum.map(source_ids, fn node_id ->
              load_fn = load_fn(node_id, surface, loads)
              :ok = Graph.register_loader(node_id, surface, load_fn)

              spawn_link(fn ->
                :ok = Graph.register_loader(node_id, surface, load_fn)
                send(parent, {:ready, cycle, node_id})

                receive do
                  {:unsubscribe, ^cycle} -> Graph.unregister(node_id)
                end
              end)
            end)

          wait_for_ready(cycle, source_ids)

          :ok = Upkeep.Invalidation.dispatch(event)
          :ok = Graph.drain()
          wait_for_values(source_ids)

          Enum.each(children, &send(&1, {:unsubscribe, cycle}))
          Enum.each(source_ids, &Graph.unregister/1)
          settle_idle(source_ids, ttl)

          System.monotonic_time(:microsecond) - started_at
        end

      %{
        latencies: latencies,
        loads: :counters.get(loads, 1),
        source_processes: SourceProcesses.count()
      }
    after
      Graph.reset()
      restore_ttl(old_ttl)
    end
  end

  defp load_fn(node_id, surface, loads) do
    fn ->
      :counters.add(loads, 1, 1)
      {{:value, node_id, :counters.get(loads, 1)}, surface}
    end
  end

  defp surface do
    InvalidationSurface.manual([{:upkeep_event, Event}], fn
      %Event{scope: :all} -> true
      _event -> false
    end)
  end

  defp wait_for_ready(cycle, source_ids) do
    source_ids
    |> MapSet.new()
    |> wait_for_ready_remaining(cycle)
  end

  defp wait_for_ready_remaining(remaining, cycle) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:ready, ^cycle, node_id} ->
          wait_for_ready_remaining(MapSet.delete(remaining, node_id), cycle)
      after
        @await_timeout_ms -> raise "did not receive #{MapSet.size(remaining)} ready message(s)"
      end
    end
  end

  defp wait_for_values(source_ids) do
    source_ids
    |> MapSet.new()
    |> wait_for_value_remaining()
  end

  defp wait_for_value_remaining(remaining) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:dag_values, pairs} ->
          remaining =
            Enum.reduce(pairs, remaining, fn {node_id, _value}, remaining ->
              MapSet.delete(remaining, node_id)
            end)

          wait_for_value_remaining(remaining)
      after
        @await_timeout_ms -> raise "did not receive #{MapSet.size(remaining)} value(s)"
      end
    end
  end

  defp drain_values do
    receive do
      {:dag_values, _pairs} -> drain_values()
    after
      0 -> :ok
    end
  end

  defp settle_idle(source_ids, 0) do
    wait_until(fn -> Enum.all?(source_ids, &(Graph.registered?(&1) == false)) end)
  end

  defp settle_idle(source_ids, :infinity) do
    wait_until(fn -> SourceProcesses.count() == length(source_ids) end)
  end

  defp print_report(source_count, cycles, rows) do
    IO.puts("source_runtime_idle_retention sources=#{source_count} cycles=#{cycles}")
    IO.puts("")
    IO.puts(format_row(["ttl", "loads", "procs", "p50_us", "p95_us", "p99_us"]))

    Enum.each(rows, fn {ttl, result} ->
      IO.puts(
        format_row([
          inspect(ttl),
          Integer.to_string(result.loads),
          Integer.to_string(result.source_processes),
          Integer.to_string(percentile(result.latencies, 0.50)),
          Integer.to_string(percentile(result.latencies, 0.95)),
          Integer.to_string(percentile(result.latencies, 0.99))
        ])
      )
    end)
  end

  defp validate!(source_count, cycles, rows) do
    expected_loads = source_count * cycles

    Enum.each(rows, fn {ttl, result} ->
      unless result.loads == expected_loads do
        raise "#{inspect(ttl)} expected #{expected_loads} loads, got #{result.loads}"
      end
    end)

    IO.puts("\nOK")
  end

  defp percentile(values, percentile) do
    values = Enum.sort(values)
    index = max(0, ceil(length(values) * percentile) - 1)
    Enum.at(values, index)
  end

  defp wait_until(fun, timeout \\ @await_timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "condition was not met before timeout"

      true ->
        Process.sleep(1)
        do_wait_until(fun, deadline)
    end
  end

  defp format_row(values) do
    values
    |> Enum.with_index()
    |> Enum.map_join(" ", fn {value, index} ->
      String.pad_trailing(value, column_width(index))
    end)
  end

  defp column_width(0), do: 12
  defp column_width(_index), do: 10

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp restore_ttl(nil), do: Application.delete_env(:upkeep, :source_idle_ttl_ms)
  defp restore_ttl(ttl), do: Application.put_env(:upkeep, :source_idle_ttl_ms, ttl)
end

Bench.SourceRuntimeIdleRetention.run()
