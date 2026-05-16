# Source runtime broad invalidation benchmark.
#
#   mix run bench/source_runtime_broad_invalidation.exs
#
# Measures the source-process runtime when one invalidation touches every
# registered source.

Application.ensure_all_started(:upkeep)

defmodule Bench.SourceRuntimeBroadInvalidation.Event do
  defstruct [:scope]
end

defmodule Bench.SourceRuntimeBroadInvalidation do
  alias Bench.SourceRuntimeBroadInvalidation.Event
  alias Upkeep.Coordinator.Graph
  alias Upkeep.InvalidationSurface

  @await_timeout_ms 30_000

  def run do
    source_count = env_int("BENCH_SOURCES", 1_000)
    iterations = env_int("BENCH_ITERATIONS", 5)

    result = run_once(source_count, iterations)

    print_report(source_count, iterations, result)
    validate!(source_count, iterations, result)
  end

  defp run_once(source_count, iterations) do
    Graph.reset()

    try do
      event = %Event{scope: :all}
      surface = broad_surface()
      loads = :counters.new(1, [:atomics])

      source_ids =
        for idx <- 1..source_count do
          node_id = {:bench_source_runtime_broad, idx}

          :ok =
            Graph.register_loader(node_id, surface, fn ->
              :counters.add(loads, 1, 1)
              {{:value, node_id, :counters.get(loads, 1)}, surface}
            end)

          node_id
        end

      latencies =
        for _run <- 1..iterations do
          drain_values()
          started_at = System.monotonic_time(:microsecond)

          :ok = Upkeep.Invalidation.dispatch(event)
          :ok = Graph.drain()
          wait_for_values(source_ids)

          System.monotonic_time(:microsecond) - started_at
        end

      Enum.each(source_ids, &Graph.unregister/1)

      %{
        latencies: latencies,
        loads: :counters.get(loads, 1),
        source_processes: source_process_count()
      }
    after
      Graph.reset()
    end
  end

  defp broad_surface do
    InvalidationSurface.manual([{:upkeep_event, Event}], fn
      %Event{scope: :all} -> true
      _event -> false
    end)
  end

  defp wait_for_values(source_ids) do
    source_ids
    |> MapSet.new()
    |> wait_for_remaining()
  end

  defp wait_for_remaining(remaining) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:dag_values, pairs} ->
          remaining =
            Enum.reduce(pairs, remaining, fn {node_id, _value}, remaining ->
              MapSet.delete(remaining, node_id)
            end)

          wait_for_remaining(remaining)
      after
        @await_timeout_ms ->
          raise "did not receive #{MapSet.size(remaining)} broad invalidation value(s)"
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

  defp print_report(source_count, iterations, result) do
    IO.puts("source_runtime_broad_invalidation sources=#{source_count} iterations=#{iterations}")
    IO.puts("")
    IO.puts(format_row(["runtime", "loads", "procs", "p50_us", "p95_us", "p99_us"]))

    IO.puts(
      format_row([
        "source_process",
        Integer.to_string(result.loads),
        Integer.to_string(result.source_processes),
        Integer.to_string(percentile(result.latencies, 0.50)),
        Integer.to_string(percentile(result.latencies, 0.95)),
        Integer.to_string(percentile(result.latencies, 0.99))
      ])
    )
  end

  defp validate!(source_count, iterations, result) do
    expected_loads = source_count * iterations

    unless result.loads == expected_loads do
      raise "expected #{expected_loads} loads, got #{result.loads}"
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

  defp source_process_count do
    case Process.whereis(Upkeep.Coordinator.SourceProcesses.Registry) do
      nil -> 0
      _pid -> Upkeep.Coordinator.SourceProcesses.count()
    end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

end

Bench.SourceRuntimeBroadInvalidation.run()
