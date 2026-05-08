# Initial connected watch source-sharing benchmark.
#
#   mix run bench/initial_source_sharing.exs
#
# Exercises the host-app path (`Live.watch/4`) rather than only the lower-level
# coordinator API. Concurrent same `{source, params}` identities should share one
# in-flight load. Distinct params should still load independently.

Code.require_file("support/initial_sharing.exs", __DIR__)

defmodule Bench.InitialSourceSharing.Event do
  defstruct [:scope]
end

defmodule Bench.InitialSourceSharing.Source do
  use Upkeep.Source

  def load(params) do
    if Map.get(params, :block?) do
      send(params.test_pid, {:bench_load_started, self(), params.run_id})

      receive do
        :continue -> :ok
      after
        5_000 -> raise "benchmark source was not released"
      end
    end

    Bench.InitialSharingSupport.bump(params.counter)
    params.value
  end

  invalidated_by(Bench.InitialSourceSharing.Event, :updated, on: :scope)
end

defmodule Bench.InitialSourceSharing do
  alias Bench.InitialSourceSharing.Source
  alias Upkeep.Live

  def run(watches) do
    test_pid = self()
    same_counter = Bench.InitialSharingSupport.counter()
    distinct_counter = Bench.InitialSharingSupport.counter()
    run_id = System.unique_integer([:positive])

    {same_us, same_loads} =
      Bench.InitialSharingSupport.timed(fn ->
        counter =
          Bench.InitialSharingSupport.telemetry_counter(
            [[:upkeep, :source, :initial_load, :coalesced]],
            fn _event, _measurements, metadata ->
              match?({Source, %{run_id: ^run_id}}, metadata.node_id)
            end
          )

        {loader_pid, tasks} =
          Bench.InitialSharingSupport.start_first_and_joiners(
            watches,
            fn -> watch_same(run_id, same_counter, test_pid) end,
            fn -> Bench.InitialSharingSupport.wait_for_started(:bench_load_started, run_id) end,
            fn expected -> Bench.InitialSharingSupport.wait_for_counter(counter, expected) end
          )

        try do
          Bench.InitialSharingSupport.release(loader_pid)
          Bench.InitialSharingSupport.await_all(tasks)
          Bench.InitialSharingSupport.counter_value(same_counter)
        after
          Bench.InitialSharingSupport.detach_telemetry_counter(counter)
        end
      end)

    {distinct_us, distinct_loads} =
      Bench.InitialSharingSupport.timed(fn ->
        Enum.each(1..watches, fn idx ->
          Bench.InitialSharingSupport.socket()
          |> Live.watch(:value, Source,
            scope: {:distinct, run_id, idx},
            run_id: run_id,
            counter: distinct_counter,
            value: {:distinct_value, run_id, idx}
          )
        end)

        Bench.InitialSharingSupport.counter_value(distinct_counter)
      end)

    %{
      watches: watches,
      same_us: same_us,
      same_loads: same_loads,
      distinct_us: distinct_us,
      distinct_loads: distinct_loads
    }
  end

  defp watch_same(run_id, counter, test_pid) do
    Bench.InitialSharingSupport.socket()
    |> Live.watch(:value, Source,
      scope: {:same, run_id},
      run_id: run_id,
      counter: counter,
      value: {:shared_value, run_id},
      block?: true,
      test_pid: test_pid
    )
  end
end

watches = Bench.InitialSharingSupport.default_watches()
result = Bench.InitialSourceSharing.run(watches)

Bench.InitialSharingSupport.print_table(
  "connected_watches=#{result.watches}",
  ["loads"],
  [
    {"same", [result.same_loads], result.same_us},
    {"distinct", [result.distinct_loads], result.distinct_us}
  ]
)

Bench.InitialSharingSupport.assert_equal!(
  result.same_loads,
  1,
  "same source identity loaded #{result.same_loads} times"
)

Bench.InitialSharingSupport.assert_equal!(
  result.distinct_loads,
  watches,
  "distinct source identities loaded #{result.distinct_loads} times"
)

Bench.InitialSharingSupport.ok()
