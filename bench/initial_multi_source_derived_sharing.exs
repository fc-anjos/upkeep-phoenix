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

Application.ensure_all_started(:upkeep)

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
    :counters.add(counter, 1, 1)

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
    ensure_table()

    test_pid = self()
    same_dashboard_counter = :counters.new(1, [:atomics])
    distinct_dashboard_counter = :counters.new(1, [:atomics])
    same_run_id = System.unique_integer([:positive])
    :ets.insert(@table, {{:counter, same_run_id, :dashboard}, same_dashboard_counter})
    :ets.insert(@table, {{:test_pid, same_run_id}, test_pid})

    {same_us, same_dashboard_computes} =
      timed(fn ->
        first =
          Task.async(fn ->
            socket()
            |> watch_dashboard_sources(same_run_id, {:same, same_run_id})
            |> Live.derive(:dashboard_model, [:stats, :activity], &Compute.dashboard/1)
          end)

        dashboard_pid =
          receive do
            {:bench_dashboard_started, dashboard_pid, ^same_run_id} -> dashboard_pid
          after
            5_000 -> raise "benchmark dashboard derive did not start"
          end

        tasks =
          for _ <- 2..watches do
            Task.async(fn ->
              socket()
              |> watch_dashboard_sources(same_run_id, {:same, same_run_id})
              |> Live.derive(:dashboard_model, [:stats, :activity], &Compute.dashboard/1)
            end)
          end

        Process.sleep(250)
        send(dashboard_pid, :continue)
        Task.await(first, 10_000)
        Enum.each(tasks, &Task.await(&1, 10_000))
        :counters.get(same_dashboard_counter, 1)
      end)

    {distinct_us, distinct_dashboard_computes} =
      timed(fn ->
        Enum.each(1..watches, fn idx ->
          run_id = System.unique_integer([:positive])
          :ets.insert(@table, {{:counter, run_id, :dashboard}, distinct_dashboard_counter})

          socket()
          |> watch_dashboard_sources(run_id, {:distinct, idx})
          |> Live.derive(:dashboard_model, [:stats, :activity], &Compute.dashboard/1)
        end)

        :counters.get(distinct_dashboard_counter, 1)
      end)

    %{
      watches: watches,
      same_us: same_us,
      same_dashboard_computes: same_dashboard_computes,
      distinct_us: distinct_us,
      distinct_dashboard_computes: distinct_dashboard_computes
    }
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

  defp timed(fun) do
    {us, result} = :timer.tc(fun)
    {us, result}
  end

  defp ensure_table do
    case :ets.info(@table) do
      :undefined -> :ets.new(@table, [:set, :public, :named_table])
      _ -> :ets.delete_all_objects(@table)
    end
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
result = Bench.InitialMultiSourceDerivedSharing.run(watches)

IO.puts("connected_multi_source_derives=#{result.watches}")
IO.puts("")

IO.puts("#{String.pad_trailing("case", 12)} #{String.pad_trailing("dashboard", 10)} elapsed_ms")

IO.puts(
  [
    String.pad_trailing("same", 12),
    String.pad_trailing(Integer.to_string(result.same_dashboard_computes), 10),
    :erlang.float_to_binary(result.same_us / 1_000, decimals: 2)
  ]
  |> Enum.join(" ")
)

IO.puts(
  [
    String.pad_trailing("distinct", 12),
    String.pad_trailing(Integer.to_string(result.distinct_dashboard_computes), 10),
    :erlang.float_to_binary(result.distinct_us / 1_000, decimals: 2)
  ]
  |> Enum.join(" ")
)

cond do
  result.same_dashboard_computes != 1 ->
    IO.puts(
      "\nFAIL: same multi-source derived identity computed #{result.same_dashboard_computes} times"
    )

    System.halt(1)

  result.distinct_dashboard_computes != watches ->
    IO.puts(
      "\nFAIL: distinct multi-source derived identities computed #{result.distinct_dashboard_computes} times"
    )

    System.halt(1)

  true ->
    IO.puts("\nOK")
end
