# Initial connected derive-sharing benchmark.
#
#   mix run bench/initial_derived_sharing.exs
#
# Exercises the host-app path (`Live.watch/4` + `Live.derive/4`). Concurrent
# identical top-level source deps plus the same external derive function should
# share one in-flight initial derived compute. Distinct source params should
# still compute independently.

Application.ensure_all_started(:upkeep)

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
    [{_, counter}] = :ets.lookup(@table, {:counter, run_id})
    :counters.add(counter, 1, 1)

    case :ets.lookup(@table, {:test_pid, run_id}) do
      [{_, test_pid}] ->
        send(test_pid, {:bench_derive_started, self(), run_id})

        receive do
          :continue -> :ok
        after
          5_000 -> raise "benchmark derive compute was not released"
        end

      [] ->
        :ok
    end

    length(issues)
  end
end

defmodule Bench.InitialDerivedSharing do
  alias Bench.InitialDerivedSharing.{Compute, Source}
  alias Upkeep.Live

  @table Bench.InitialDerivedSharing.Table

  def run(watches) do
    ensure_table()

    test_pid = self()
    same_counter = :counters.new(1, [:atomics])
    distinct_counter = :counters.new(1, [:atomics])
    same_run_id = System.unique_integer([:positive])
    :ets.insert(@table, {{:counter, same_run_id}, same_counter})
    :ets.insert(@table, {{:test_pid, same_run_id}, test_pid})

    {same_us, same_computes} =
      timed(fn ->
        first =
          Task.async(fn ->
            socket()
            |> Live.watch(:issues, Source,
              scope: {:same, same_run_id},
              value: [{same_run_id, :issue}]
            )
            |> Live.derive(:issue_count, [:issues], &Compute.count/1)
          end)

        loader_pid =
          receive do
            {:bench_derive_started, loader_pid, ^same_run_id} -> loader_pid
          after
            5_000 -> raise "benchmark initial derive did not start"
          end

        tasks =
          for _ <- 2..watches do
            Task.async(fn ->
              socket()
              |> Live.watch(:issues, Source,
                scope: {:same, same_run_id},
                value: [{same_run_id, :issue}]
              )
              |> Live.derive(:issue_count, [:issues], &Compute.count/1)
            end)
          end

        Process.sleep(250)
        send(loader_pid, :continue)
        Task.await(first, 10_000)
        Enum.each(tasks, &Task.await(&1, 10_000))
        :counters.get(same_counter, 1)
      end)

    {distinct_us, distinct_computes} =
      timed(fn ->
        Enum.each(1..watches, fn idx ->
          run_id = System.unique_integer([:positive])
          :ets.insert(@table, {{:counter, run_id}, distinct_counter})

          socket()
          |> Live.watch(:issues, Source,
            scope: {:distinct, idx},
            value: [{run_id, :issue}]
          )
          |> Live.derive(:issue_count, [:issues], &Compute.count/1)
        end)

        :counters.get(distinct_counter, 1)
      end)

    %{
      watches: watches,
      same_us: same_us,
      same_computes: same_computes,
      distinct_us: distinct_us,
      distinct_computes: distinct_computes
    }
  end

  defp timed(fun) do
    {us, computes} = :timer.tc(fun)
    {us, computes}
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
result = Bench.InitialDerivedSharing.run(watches)

IO.puts("connected_derives=#{result.watches}")
IO.puts("")
IO.puts("#{String.pad_trailing("case", 12)} #{String.pad_trailing("computes", 8)} elapsed_ms")

IO.puts(
  [
    String.pad_trailing("same", 12),
    String.pad_trailing(Integer.to_string(result.same_computes), 8),
    :erlang.float_to_binary(result.same_us / 1_000, decimals: 2)
  ]
  |> Enum.join(" ")
)

IO.puts(
  [
    String.pad_trailing("distinct", 12),
    String.pad_trailing(Integer.to_string(result.distinct_computes), 8),
    :erlang.float_to_binary(result.distinct_us / 1_000, decimals: 2)
  ]
  |> Enum.join(" ")
)

cond do
  result.same_computes != 1 ->
    IO.puts("\nFAIL: same derived identity computed #{result.same_computes} times")
    System.halt(1)

  result.distinct_computes != watches ->
    IO.puts("\nFAIL: distinct derived identities computed #{result.distinct_computes} times")
    System.halt(1)

  true ->
    IO.puts("\nOK")
end
