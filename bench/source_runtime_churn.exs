# Source runtime churn benchmark.
#
#   mix run bench/source_runtime_churn.exs
#
# Measures many short-lived source identities. This is the source-process
# runtime's unfavorable case: it must create and stop a supervised process for
# every identity.

Application.ensure_all_started(:upkeep)

defmodule Bench.SourceRuntimeChurn.Event do
  defstruct [:scope]
end

defmodule Bench.SourceRuntimeChurn do
  alias Bench.SourceRuntimeChurn.Event
  alias Upkeep.Coordinator.Graph
  alias Upkeep.InvalidationSurface

  @await_timeout_ms 30_000

  def run do
    source_count = env_int("BENCH_CHURN_SOURCES", env_int("BENCH_SOURCES", 1_000))
    iterations = env_int("BENCH_ITERATIONS", 5)

    result = run_once(source_count, iterations)

    print_report(source_count, iterations, result)
    validate!(result)
  end

  defp run_once(source_count, iterations) do
    Graph.reset()

    try do
      surface = surface()

      samples =
        for run <- 1..iterations do
          node_ids =
            for idx <- 1..source_count do
              {:bench_source_runtime_churn, run, idx}
            end

          started_at = System.monotonic_time(:microsecond)

          Enum.each(node_ids, fn node_id ->
            :ok = Graph.register_loader(node_id, surface, fn -> {{:value, node_id}, surface} end)
          end)

          registered_at = System.monotonic_time(:microsecond)

          Enum.each(node_ids, &Graph.unregister/1)
          wait_until_unregistered(node_ids)

          stopped_at = System.monotonic_time(:microsecond)

          %{
            register_us: registered_at - started_at,
            unregister_us: stopped_at - registered_at,
            total_us: stopped_at - started_at
          }
        end

      %{
        register_latencies: Enum.map(samples, & &1.register_us),
        unregister_latencies: Enum.map(samples, & &1.unregister_us),
        total_latencies: Enum.map(samples, & &1.total_us),
        source_processes: source_process_count()
      }
    after
      Graph.reset()
    end
  end

  defp surface do
    InvalidationSurface.manual([{:upkeep_event, Event}], fn
      %Event{} -> false
      _event -> false
    end)
  end

  defp wait_until_unregistered(node_ids) do
    deadline = System.monotonic_time(:millisecond) + @await_timeout_ms
    do_wait_until_unregistered(node_ids, deadline)
  end

  defp do_wait_until_unregistered(node_ids, deadline) do
    remaining = Enum.filter(node_ids, &Graph.registered?/1)

    cond do
      remaining == [] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "timed out waiting for #{length(remaining)} source registration(s) to clean up"

      true ->
        Process.sleep(1)
        do_wait_until_unregistered(remaining, deadline)
    end
  end

  defp print_report(source_count, iterations, result) do
    IO.puts("source_runtime_churn sources=#{source_count} iterations=#{iterations}")
    IO.puts("")

    IO.puts(
      format_row([
        "runtime",
        "procs",
        "reg_p50",
        "reg_p95",
        "unreg_p50",
        "unreg_p95",
        "total_p50",
        "total_p95"
      ])
    )

    IO.puts(
      format_row([
        "source_process",
        Integer.to_string(result.source_processes),
        Integer.to_string(percentile(result.register_latencies, 0.50)),
        Integer.to_string(percentile(result.register_latencies, 0.95)),
        Integer.to_string(percentile(result.unregister_latencies, 0.50)),
        Integer.to_string(percentile(result.unregister_latencies, 0.95)),
        Integer.to_string(percentile(result.total_latencies, 0.50)),
        Integer.to_string(percentile(result.total_latencies, 0.95))
      ])
    )
  end

  defp validate!(result) do
    unless result.source_processes == 0 do
      raise "left #{result.source_processes} source process(es) after churn"
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
  defp column_width(_index), do: 12

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

Bench.SourceRuntimeChurn.run()
