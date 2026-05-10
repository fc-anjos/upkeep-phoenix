# Distinct watch mount regression gate.
#
#   mix run bench/distinct_watch_mount.exs
#
# Mounts many distinct source watches into one socket. Adding a brand-new DAG
# node cannot introduce a cycle because existing nodes have no path through an
# absent node, so this guards against accidentally revalidating the whole graph
# on every new watch.

Application.ensure_all_started(:upkeep)

defmodule Bench.DistinctWatchMount.Event do
  defstruct [:scope]
end

defmodule Bench.DistinctWatchMount.Source do
  use Upkeep.Source

  def load(params), do: params.value

  invalidated_by(Bench.DistinctWatchMount.Event, :updated, on: :scope)
end

defmodule Bench.DistinctWatchMount do
  alias Bench.DistinctWatchMount.Source

  def run(count) when is_integer(count) and count > 0 do
    base = %Phoenix.LiveView.Socket{
      endpoint: UpkeepWeb.Endpoint,
      view: UpkeepWeb.KanbanLive,
      transport_pid: self(),
      assigns: %{__changed__: %{}}
    }

    {elapsed_us, socket} =
      :timer.tc(fn ->
        Enum.reduce(1..count, base, fn idx, socket ->
          Upkeep.Live.watch(socket, assign_name(idx), Source, scope: idx, value: idx)
        end)
      end)

    %{
      count: count,
      elapsed_us: elapsed_us,
      avg_us: elapsed_us / count,
      assigns: map_size(socket.assigns)
    }
  end

  defp assign_name(idx), do: String.to_atom("item_#{idx}")
end

count =
  case System.get_env("BENCH_WATCHES") do
    nil -> 2_000
    value -> String.to_integer(value)
  end

result = Bench.DistinctWatchMount.run(count)

IO.puts(
  "distinct_watch_mount count=#{result.count} total_ms=#{Float.round(result.elapsed_us / 1_000, 2)} avg_us=#{Float.round(result.avg_us, 2)} assigns=#{result.assigns}"
)

max_avg_us = 500

if result.avg_us <= max_avg_us do
  IO.puts("OK")
else
  IO.puts("FAIL: avg_us #{Float.round(result.avg_us, 2)} > #{max_avg_us}")
  System.halt(1)
end
