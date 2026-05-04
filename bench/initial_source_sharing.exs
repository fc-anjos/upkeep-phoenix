# Initial connected watch source-sharing benchmark.
#
#   mix run bench/initial_source_sharing.exs
#
# Exercises the host-app path (`Live.watch/4`) rather than only the lower-level
# coordinator API. Concurrent same `{source, params}` identities should share one
# in-flight load. Distinct params should still load independently.

Application.ensure_all_started(:upkeep)

defmodule Bench.InitialSourceSharing.Event do
  defstruct [:scope]
end

defmodule Bench.InitialSourceSharing.Source do
  use Upkeep.Source

  def load(params) do
    if Map.get(params, :block?) do
      send(params.test_pid, {:bench_load_started, self()})

      receive do
        :continue -> :ok
      after
        5_000 -> raise "benchmark source was not released"
      end
    end

    :counters.add(params.counter, 1, 1)
    params.value
  end

  invalidated_by(Bench.InitialSourceSharing.Event, :updated, on: :scope)
end

defmodule Bench.InitialSourceSharing do
  alias Bench.InitialSourceSharing.Source
  alias Upkeep.Live

  def run(watches) do
    test_pid = self()
    same_counter = :counters.new(1, [:atomics])
    distinct_counter = :counters.new(1, [:atomics])
    run_id = System.unique_integer([:positive])

    {same_us, same_loads} =
      timed(fn ->
        first =
          Task.async(fn ->
            socket()
            |> Live.watch(:value, Source,
              scope: {:same, run_id},
              counter: same_counter,
              value: {:shared_value, run_id},
              block?: true,
              test_pid: test_pid
            )
          end)

        loader_pid =
          receive do
            {:bench_load_started, loader_pid} -> loader_pid
          after
            5_000 -> raise "benchmark initial load did not start"
          end

        loader_pid
        |> then(fn loader_pid ->
          tasks =
            for _ <- 2..watches do
              Task.async(fn ->
                socket()
                |> Live.watch(:value, Source,
                  scope: {:same, run_id},
                  counter: same_counter,
                  value: {:shared_value, run_id},
                  block?: true,
                  test_pid: test_pid
                )
              end)
            end

          # Give the spawned connected watches time to join the in-flight load.
          # This benchmark intentionally measures single-flight behavior, not
          # sequential cache reuse.
          Process.sleep(250)
          send(loader_pid, :continue)
          Task.await(first, 10_000)
          Enum.each(tasks, &Task.await(&1, 10_000))
        end)

        :counters.get(same_counter, 1)
      end)

    {distinct_us, distinct_loads} =
      timed(fn ->
        Enum.each(1..watches, fn idx ->
          socket()
          |> Live.watch(:value, Source,
            scope: {:distinct, run_id, idx},
            counter: distinct_counter,
            value: {:distinct_value, run_id, idx}
          )
        end)

        :counters.get(distinct_counter, 1)
      end)

    %{
      watches: watches,
      same_us: same_us,
      same_loads: same_loads,
      distinct_us: distinct_us,
      distinct_loads: distinct_loads
    }
  end

  defp timed(fun) do
    {us, loads} = :timer.tc(fun)
    {us, loads}
  end

  defp socket do
    %Phoenix.LiveView.Socket{
      endpoint: UpkeepWeb.Endpoint,
      view: UpkeepWeb.KanbanLive,
      transport_pid: self(),
      assigns: %{__changed__: %{}}
    }
  end
end

watches = 1_000
result = Bench.InitialSourceSharing.run(watches)

IO.puts("connected_watches=#{result.watches}")
IO.puts("")
IO.puts("#{String.pad_trailing("case", 12)} #{String.pad_trailing("loads", 8)} elapsed_ms")

IO.puts(
  [
    String.pad_trailing("same", 12),
    String.pad_trailing(Integer.to_string(result.same_loads), 8),
    :erlang.float_to_binary(result.same_us / 1_000, decimals: 2)
  ]
  |> Enum.join(" ")
)

IO.puts(
  [
    String.pad_trailing("distinct", 12),
    String.pad_trailing(Integer.to_string(result.distinct_loads), 8),
    :erlang.float_to_binary(result.distinct_us / 1_000, decimals: 2)
  ]
  |> Enum.join(" ")
)

cond do
  result.same_loads != 1 ->
    IO.puts("\nFAIL: same source identity loaded #{result.same_loads} times")
    System.halt(1)

  result.distinct_loads != watches ->
    IO.puts("\nFAIL: distinct source identities loaded #{result.distinct_loads} times")
    System.halt(1)

  true ->
    IO.puts("\nOK")
end
