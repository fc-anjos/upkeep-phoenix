# Initial connected derive-sharing benchmark.
#
#   mix run bench/initial_derived_sharing.exs
#
# Exercises the host-app path (`Live.watch/4` + `Live.derive/4`). Concurrent
# identical top-level source deps plus the same external derive function should
# share one in-flight initial derived compute. Distinct source params should
# still compute independently.

Code.require_file("support/initial_sharing.exs", __DIR__)

defmodule Bench.InitialDerivedSharing.Event do
  defstruct [:scope]
end

defmodule Bench.InitialDerivedSharing.Source do
  use Upkeep.Source

  def load(params), do: params.value

  invalidated_by(Bench.InitialDerivedSharing.Event, :updated, on: :scope)
end

defmodule Bench.InitialDerivedSharing.Compute do
  @table Bench.InitialDerivedSharing.Table

  def count(%{issues: [{run_id, _issue} | _] = issues}) do
    [{_, counter}] = :ets.lookup(@table, {:counter, run_id, :count})
    Bench.InitialSharingSupport.bump(counter)

    case :ets.lookup(@table, {:test_pid, run_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:bench_count_started, self(), run_id})

        receive do
          :continue -> :ok
        after
          5_000 -> raise "benchmark derive compute was not released"
        end

      [] ->
        :ok
    end

    %{run_id: run_id, count: length(issues)}
  end

  def label(%{issue_count: %{run_id: run_id, count: count}}) do
    [{_, counter}] = :ets.lookup(@table, {:counter, run_id, :label})
    Bench.InitialSharingSupport.bump(counter)

    case :ets.lookup(@table, {:test_pid, run_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:bench_label_started, self(), run_id})

        receive do
          :continue -> :ok
        after
          5_000 -> raise "benchmark chained derive compute was not released"
        end

      [] ->
        :ok
    end

    "#{count} issue"
  end
end

defmodule Bench.InitialDerivedSharing do
  alias Bench.InitialDerivedSharing.{Compute, Source}
  alias Upkeep.Live

  @table Bench.InitialDerivedSharing.Table

  def run(watches) do
    Bench.InitialSharingSupport.ensure_table(@table)

    test_pid = self()
    same_count_counter = Bench.InitialSharingSupport.counter()
    same_label_counter = Bench.InitialSharingSupport.counter()
    distinct_count_counter = Bench.InitialSharingSupport.counter()
    distinct_label_counter = Bench.InitialSharingSupport.counter()
    same_run_id = System.unique_integer([:positive])
    :ets.insert(@table, {{:counter, same_run_id, :count}, same_count_counter})
    :ets.insert(@table, {{:counter, same_run_id, :label}, same_label_counter})
    :ets.insert(@table, {{:test_pid, same_run_id}, test_pid})

    {same_us, {same_count_computes, same_label_computes}} =
      Bench.InitialSharingSupport.timed(fn ->
        count_counter = derived_hit_counter(:issue_count)
        label_counter = derived_hit_counter(:issue_label)

        {count_pid, tasks} =
          Bench.InitialSharingSupport.start_first_and_joiners(
            watches,
            fn -> watch_chain(same_run_id) end,
            fn ->
              Bench.InitialSharingSupport.wait_for_started(:bench_count_started, same_run_id)
            end,
            fn expected ->
              Bench.InitialSharingSupport.wait_for_counter(count_counter, expected)
            end
          )

        try do
          Bench.InitialSharingSupport.release(count_pid)

          label_pid =
            Bench.InitialSharingSupport.wait_for_started(:bench_label_started, same_run_id)

          Bench.InitialSharingSupport.wait_for_counter(label_counter, max(watches - 1, 0))
          Bench.InitialSharingSupport.release(label_pid)

          Bench.InitialSharingSupport.await_all(tasks)

          {
            Bench.InitialSharingSupport.counter_value(same_count_counter),
            Bench.InitialSharingSupport.counter_value(same_label_counter)
          }
        after
          Bench.InitialSharingSupport.detach_telemetry_counter(count_counter)
          Bench.InitialSharingSupport.detach_telemetry_counter(label_counter)
        end
      end)

    {distinct_us, {distinct_count_computes, distinct_label_computes}} =
      Bench.InitialSharingSupport.timed(fn ->
        Bench.InitialSharingSupport.parallel_each(1..watches, fn idx ->
          run_id = System.unique_integer([:positive])
          :ets.insert(@table, {{:counter, run_id, :count}, distinct_count_counter})
          :ets.insert(@table, {{:counter, run_id, :label}, distinct_label_counter})

          watch_chain(run_id, {:distinct, idx})
        end)

        {
          Bench.InitialSharingSupport.counter_value(distinct_count_counter),
          Bench.InitialSharingSupport.counter_value(distinct_label_counter)
        }
      end)

    %{
      watches: watches,
      same_us: same_us,
      same_count_computes: same_count_computes,
      same_label_computes: same_label_computes,
      distinct_us: distinct_us,
      distinct_count_computes: distinct_count_computes,
      distinct_label_computes: distinct_label_computes
    }
  end

  defp watch_chain(run_id, scope \\ nil)

  defp watch_chain(run_id, nil), do: watch_chain(run_id, {:same, run_id})

  defp watch_chain(run_id, scope) do
    Bench.InitialSharingSupport.socket()
    |> Live.watch(:issues, Source,
      scope: scope,
      value: [{run_id, :issue}]
    )
    |> Live.derive(:issue_count, [:issues], &Compute.count/1)
    |> Live.derive(:issue_label, [:issue_count], &Compute.label/1)
  end

  defp derived_hit_counter(assign_name) do
    Bench.InitialSharingSupport.telemetry_counter(
      [[:upkeep, :graph, :derived_initial, :hit]],
      fn _event, _measurements, metadata ->
        metadata.assign_name == assign_name
      end
    )
  end
end

watches = Bench.InitialSharingSupport.default_watches()
result = Bench.InitialDerivedSharing.run(watches)

Bench.InitialSharingSupport.print_table(
  "connected_derives=#{result.watches}",
  ["count", "label"],
  [
    {"same", [result.same_count_computes, result.same_label_computes], result.same_us},
    {"distinct", [result.distinct_count_computes, result.distinct_label_computes],
     result.distinct_us}
  ]
)

Bench.InitialSharingSupport.assert_equal!(
  {result.same_count_computes, result.same_label_computes},
  {1, 1},
  "same derived chain computed count=#{result.same_count_computes} label=#{result.same_label_computes} times"
)

Bench.InitialSharingSupport.assert_equal!(
  {result.distinct_count_computes, result.distinct_label_computes},
  {watches, watches},
  "distinct derived chains computed count=#{result.distinct_count_computes} label=#{result.distinct_label_computes} times"
)

Bench.InitialSharingSupport.ok()
