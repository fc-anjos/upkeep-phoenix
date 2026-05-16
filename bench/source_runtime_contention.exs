# Source process contention benchmark.
#
#   mix run bench/source_runtime_contention.exs
#
# Measures tail latency when a slow source refresh is in flight and an
# unrelated fast source is invalidated at the same time.

Application.ensure_all_started(:upkeep)

defmodule Bench.SourceRuntimeContention.Event do
  defstruct [:id]
end

defmodule Bench.SourceRuntimeContention do
  alias Bench.SourceRuntimeContention.Event
  alias Upkeep.Coordinator.Graph
  alias Upkeep.Coordinator.Graph.Notifier
  alias Upkeep.InvalidationSurface

  @await_timeout_ms 10_000

  def run do
    iterations = env_int("BENCH_ITERATIONS", 40)
    slow_ms = env_int("BENCH_SLOW_MS", 25)

    result = run_once(iterations, slow_ms)
    print_report(iterations, slow_ms, result)
    validate!(iterations, result)
  end

  defp run_once(iterations, slow_ms) do
    Graph.reset()

    try do
      parent = self()
      slow_id = {:bench_source_runtime_contention, :slow}
      fast_id = {:bench_source_runtime_contention, :fast}
      slow_event = %Event{id: :slow}
      fast_event = %Event{id: :fast}
      slow_surface = surface(slow_event)
      fast_surface = surface(fast_event)
      slow_loads = :counters.new(1, [:atomics])
      fast_loads = :counters.new(1, [:atomics])

      :ok =
        Graph.register_loader(slow_id, slow_surface, fn ->
          :counters.add(slow_loads, 1, 1)
          run = :counters.get(slow_loads, 1)
          send(parent, {:slow_started, run, self()})

          receive do
            {:release_slow, ^run} -> {{:slow_value, run}, slow_surface}
          after
            @await_timeout_ms -> raise "slow source was not released"
          end
        end)

      :ok =
        Graph.register_loader(fast_id, fast_surface, fn ->
          :counters.add(fast_loads, 1, 1)
          {{:fast_value, :counters.get(fast_loads, 1)}, fast_surface}
        end)

      latencies =
        for run <- 1..iterations do
          latency_us = measure_fast_latency_us(run, slow_ms, slow_event, fast_event, fast_id)
          Graph.drain()
          drain_values()
          latency_us
        end

      Graph.unregister(slow_id)
      Graph.unregister(fast_id)

      %{
        latencies: latencies,
        slow_loads: :counters.get(slow_loads, 1),
        fast_loads: :counters.get(fast_loads, 1)
      }
    after
      Graph.reset()
    end
  end

  defp measure_fast_latency_us(run, slow_ms, slow_event, fast_event, fast_id) do
    :ok = notify_and_flush(slow_event)
    slow_loader = wait_for_slow(run)

    releaser =
      Task.async(fn ->
        Process.sleep(slow_ms)
        send(slow_loader, {:release_slow, run})
      end)

    started_at = System.monotonic_time(:microsecond)

    :ok = notify_and_flush(fast_event)
    wait_for_value(fast_id)

    latency_us = System.monotonic_time(:microsecond) - started_at
    Task.await(releaser, @await_timeout_ms)
    latency_us
  end

  defp notify_and_flush(event) do
    :ok = Upkeep.Invalidation.dispatch(event)
    Notifier.drain()
  end

  defp wait_for_slow(run) do
    receive do
      {:slow_started, ^run, pid} -> pid
    after
      @await_timeout_ms -> raise "slow source did not start run #{run}"
    end
  end

  defp wait_for_value(node_id) do
    receive do
      {:dag_values, pairs} ->
        if Enum.any?(pairs, &match?({^node_id, _value}, &1)) do
          :ok
        else
          wait_for_value(node_id)
        end
    after
      @await_timeout_ms -> raise "did not receive value for #{inspect(node_id)}"
    end
  end

  defp drain_values do
    receive do
      {:dag_values, _pairs} -> drain_values()
    after
      0 -> :ok
    end
  end

  defp surface(%Event{} = expected) do
    InvalidationSurface.manual([{:upkeep_event, Event}], fn
      %Event{id: id} -> id == expected.id
      _event -> false
    end)
  end

  defp percentile(values, percentile) do
    values = Enum.sort(values)
    index = max(0, ceil(length(values) * percentile) - 1)
    Enum.at(values, index)
  end

  defp print_report(iterations, slow_ms, result) do
    IO.puts("source_runtime_contention iterations=#{iterations} slow_ms=#{slow_ms}")
    IO.puts("")
    IO.puts(format_row(["runtime", "slow", "fast", "p50_us", "p95_us", "p99_us"]))

    IO.puts(
      format_row([
        "source_process",
        Integer.to_string(result.slow_loads),
        Integer.to_string(result.fast_loads),
        Integer.to_string(percentile(result.latencies, 0.50)),
        Integer.to_string(percentile(result.latencies, 0.95)),
        Integer.to_string(percentile(result.latencies, 0.99))
      ])
    )
  end

  defp validate!(iterations, result) do
    unless result.slow_loads == iterations and result.fast_loads == iterations do
      raise "expected #{iterations} slow and fast loads, got " <>
              "#{result.slow_loads} slow and #{result.fast_loads} fast"
    end

    IO.puts("\nOK")
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

Bench.SourceRuntimeContention.run()
