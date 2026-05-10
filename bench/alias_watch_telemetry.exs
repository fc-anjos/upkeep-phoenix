# Alias watch telemetry regression gate.
#
#   mix run bench/alias_watch_telemetry.exs
#
# Repeatedly aliases the same watched source under many assign names. This
# used to be dominated by building telemetry metadata that sorted the full
# assign-name set on every alias event, making setup effectively quadratic.

Application.ensure_all_started(:upkeep)

defmodule Bench.AliasWatchTelemetry.Event do
  defstruct [:scope]
end

defmodule Bench.AliasWatchTelemetry.Source do
  use Upkeep.Source

  def load(params), do: params.value

  invalidated_by(Bench.AliasWatchTelemetry.Event, :updated, on: :scope)
end

defmodule Bench.AliasWatchTelemetry do
  alias Bench.AliasWatchTelemetry.Source

  def run(aliases) when is_integer(aliases) and aliases > 0 do
    base = %Phoenix.LiveView.Socket{
      endpoint: UpkeepWeb.Endpoint,
      view: UpkeepWeb.KanbanLive,
      transport_pid: self(),
      assigns: %{__changed__: %{}, current_scope: %{user_id: 1}}
    }

    socket = Upkeep.Live.watch(base, :item_0, Source, scope: 1, value: :ok)

    {elapsed_us, socket} =
      :timer.tc(fn ->
        Enum.reduce(1..aliases, socket, fn idx, socket ->
          Upkeep.Live.watch(socket, assign_name(idx), Source, scope: 1, value: :ok)
        end)
      end)

    %{
      aliases: aliases,
      elapsed_us: elapsed_us,
      avg_us: elapsed_us / aliases,
      assigns: map_size(socket.assigns)
    }
  end

  defp assign_name(idx), do: String.to_atom("item_#{idx}")
end

aliases =
  case System.get_env("BENCH_ALIASES") do
    nil -> 5_000
    value -> String.to_integer(value)
  end

result = Bench.AliasWatchTelemetry.run(aliases)

IO.puts(
  "alias_watch_telemetry aliases=#{result.aliases} total_ms=#{Float.round(result.elapsed_us / 1_000, 2)} avg_us=#{Float.round(result.avg_us, 2)} assigns=#{result.assigns}"
)

max_avg_us = 400

if result.avg_us <= max_avg_us do
  IO.puts("OK")
else
  IO.puts("FAIL: avg_us #{Float.round(result.avg_us, 2)} > #{max_avg_us}")
  System.halt(1)
end
