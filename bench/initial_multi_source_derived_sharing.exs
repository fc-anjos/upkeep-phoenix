# Initial connected multi-source derive-sharing benchmark.
#
#   mix run bench/initial_multi_source_derived_sharing.exs
#
# Exercises a dashboard-shaped host-app path:
#
#   watch(:stats, Source, scope: ...)
#   watch(:activity, Source, scope: ...)
#   derive(:dashboard_model, [:stats, :activity], &Compute.dashboard/1)
#
# Concurrent identical source identities should share one in-flight initial
# dashboard compute. Distinct source identities should still compute
# independently.

Code.require_file("support/initial_sharing.exs", __DIR__)

defmodule Bench.InitialMultiSourceDerivedSharing.Event do
  defstruct [:scope]
end

defmodule Bench.InitialMultiSourceDerivedSharing.Source do
  use Upkeep.Source

  def load(params), do: params.value

  invalidated_by(Bench.InitialMultiSourceDerivedSharing.Event, :updated, on: :scope)
end

defmodule Bench.InitialMultiSourceDerivedSharing.Compute do
  @table Bench.InitialMultiSourceDerivedSharing.Table

  def dashboard(%{stats: %{run_id: run_id, stats: stats}, activity: activity}) do
    [{_, counter}] = :ets.lookup(@table, {:counter, run_id, :dashboard})
    Bench.InitialSharingSupport.bump(counter)

    case :ets.lookup(@table, {:test_pid, run_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:bench_dashboard_started, self(), run_id})

        receive do
          :continue -> :ok
        after
          5_000 -> raise "benchmark dashboard compute was not released"
        end

      [] ->
        :ok
    end

    %{run_id: run_id, score: stats.score, activity_count: length(activity.items)}
  end
end

defmodule Bench.InitialMultiSourceDerivedSharing do
  alias Bench.InitialMultiSourceDerivedSharing.{Compute, Source}
  alias Upkeep.Live

  @table Bench.InitialMultiSourceDerivedSharing.Table

  def run(watches) do
    Bench.InitialSharingSupport.ensure_table(@table)

    test_pid = self()
    same_dashboard_counter = Bench.InitialSharingSupport.counter()
    distinct_dashboard_counter = Bench.InitialSharingSupport.counter()
    same_run_id = System.unique_integer([:positive])
    :ets.insert(@table, {{:counter, same_run_id, :dashboard}, same_dashboard_counter})
    :ets.insert(@table, {{:test_pid, same_run_id}, test_pid})

    {same_us, same_dashboard_computes} =
      Bench.InitialSharingSupport.timed(fn ->
        {dashboard_pid, tasks} =
          Bench.InitialSharingSupport.start_first_and_joiners(
            watches,
            fn -> watch_dashboard(same_run_id, {:same, same_run_id}) end,
            fn ->
              Bench.InitialSharingSupport.wait_for_started(
                :bench_dashboard_started,
                same_run_id
              )
            end
          )

        Bench.InitialSharingSupport.release(dashboard_pid)
        Bench.InitialSharingSupport.await_all(tasks)
        Bench.InitialSharingSupport.counter_value(same_dashboard_counter)
      end)

    {distinct_us, distinct_dashboard_computes} =
      Bench.InitialSharingSupport.timed(fn ->
        Enum.each(1..watches, fn idx ->
          run_id = System.unique_integer([:positive])
          :ets.insert(@table, {{:counter, run_id, :dashboard}, distinct_dashboard_counter})

          watch_dashboard(run_id, {:distinct, idx})
        end)

        Bench.InitialSharingSupport.counter_value(distinct_dashboard_counter)
      end)

    %{
      watches: watches,
      same_us: same_us,
      same_dashboard_computes: same_dashboard_computes,
      distinct_us: distinct_us,
      distinct_dashboard_computes: distinct_dashboard_computes
    }
  end

  defp watch_dashboard(run_id, scope) do
    Bench.InitialSharingSupport.socket()
    |> watch_dashboard_sources(run_id, scope)
    |> Live.derive(:dashboard_model, [:stats, :activity], &Compute.dashboard/1)
  end

  defp watch_dashboard_sources(socket, run_id, scope) do
    socket
    |> Live.watch(:stats, Source,
      scope: {:stats, scope},
      value: %{run_id: run_id, stats: %{score: 42}}
    )
    |> Live.watch(:activity, Source,
      scope: {:activity, scope},
      value: %{run_id: run_id, items: [:deploy, :comment]}
    )
  end
end

watches = Bench.InitialSharingSupport.default_watches()
result = Bench.InitialMultiSourceDerivedSharing.run(watches)

Bench.InitialSharingSupport.print_table(
  "connected_multi_source_derives=#{result.watches}",
  ["dashboard"],
  [
    {"same", [result.same_dashboard_computes], result.same_us},
    {"distinct", [result.distinct_dashboard_computes], result.distinct_us}
  ]
)

Bench.InitialSharingSupport.assert_equal!(
  result.same_dashboard_computes,
  1,
  "same multi-source derived identity computed #{result.same_dashboard_computes} times"
)

Bench.InitialSharingSupport.assert_equal!(
  result.distinct_dashboard_computes,
  watches,
  "distinct multi-source derived identities computed #{result.distinct_dashboard_computes} times"
)

Bench.InitialSharingSupport.ok()
