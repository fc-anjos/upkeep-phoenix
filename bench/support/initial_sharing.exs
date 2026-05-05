Application.ensure_all_started(:upkeep)

defmodule Bench.InitialSharingSupport do
  @moduledoc false

  @default_watches 1_000
  @join_window_ms 250
  @await_timeout_ms 10_000
  @default_parallelism System.schedulers_online()

  def default_watches do
    case System.get_env("BENCH_WATCHES") do
      nil -> @default_watches
      value -> String.to_integer(value)
    end
  end

  def socket do
    %Phoenix.LiveView.Socket{
      endpoint: UpkeepWeb.Endpoint,
      view: UpkeepWeb.KanbanLive,
      transport_pid: self(),
      assigns: %{__changed__: %{}}
    }
  end

  def counter, do: :counters.new(1, [:atomics])
  def counter_value(counter), do: :counters.get(counter, 1)
  def bump(counter), do: :counters.add(counter, 1, 1)

  def timed(fun) do
    {us, result} = :timer.tc(fun)
    {us, result}
  end

  def ensure_table(table) do
    case :ets.info(table) do
      :undefined -> :ets.new(table, [:set, :public, :named_table])
      _ -> :ets.delete_all_objects(table)
    end
  end

  def start_first_and_joiners(watches, fun, wait_for_first) do
    first = Task.async(fun)
    first_pid = wait_for_first.()

    tasks =
      for _ <- 2..watches do
        Task.async(fun)
      end

    {first_pid, [first | tasks]}
  end

  def release(pid) do
    Process.sleep(@join_window_ms)
    send(pid, :continue)
  end

  def await_all(tasks), do: Enum.each(tasks, &Task.await(&1, @await_timeout_ms))

  def parallel_each(enumerable, fun) do
    enumerable
    |> Task.async_stream(
      fun,
      max_concurrency: parallelism(),
      timeout: @await_timeout_ms,
      ordered: false
    )
    |> Enum.each(fn
      {:ok, _result} -> :ok
      {:exit, reason} -> exit(reason)
    end)
  end

  def parallelism do
    case System.get_env("BENCH_PARALLELISM") do
      nil -> @default_parallelism
      value -> String.to_integer(value)
    end
  end

  def wait_for_started(tag, run_id, timeout \\ 5_000) do
    receive do
      {^tag, pid, ^run_id} -> pid
    after
      timeout -> raise "benchmark #{inspect(tag)} did not start"
    end
  end

  def print_table(title, columns, rows) do
    IO.puts(title)
    IO.puts("")
    IO.puts(format_row(["case" | columns] ++ ["elapsed_ms"]))

    Enum.each(rows, fn {name, values, elapsed_us} ->
      elapsed_ms = :erlang.float_to_binary(elapsed_us / 1_000, decimals: 2)
      IO.puts(format_row([name | Enum.map(values, &to_string/1)] ++ [elapsed_ms]))
    end)
  end

  def assert_equal!(actual, expected, message) do
    unless actual == expected do
      IO.puts("\nFAIL: #{message}")
      System.halt(1)
    end
  end

  def ok, do: IO.puts("\nOK")

  defp format_row(values) do
    values
    |> Enum.with_index()
    |> Enum.map_join(" ", fn {value, index} ->
      String.pad_trailing(value, column_width(index))
    end)
  end

  defp column_width(0), do: 12
  defp column_width(_index), do: 10
end
