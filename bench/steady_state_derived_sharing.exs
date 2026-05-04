# Steady-state connected derive-sharing benchmark.
#
#   mix run bench/steady_state_derived_sharing.exs
#
# Exercises the post-mount update path:
#
#   watch(:items, Source, scope: ...)
#   derive(:count, [:items], &Compute.count/1)
#   notify(Event.updated(scope))
#
# For many subscribers on the same source identity, one invalidation should
# reload the source once, recompute the shared derived value once, and deliver
# the graph-pushed source + derived values to every subscriber.

Code.require_file("support/initial_sharing.exs", __DIR__)

defmodule Bench.SteadyStateDerivedSharing.Event do
  defstruct [:scope]
end

defmodule Bench.SteadyStateDerivedSharing.Source do
  use Upkeep.Source

  def load(params) do
    [{_, source_loads}] = :ets.lookup(Bench.SteadyStateDerivedSharing.Table, :source_loads)
    Bench.InitialSharingSupport.bump(source_loads)
    :ets.lookup_element(Bench.SteadyStateDerivedSharing.Table, {:value, params.scope}, 2)
  end

  invalidated_by(Bench.SteadyStateDerivedSharing.Event, :updated, on: :scope)
end

defmodule Bench.SteadyStateDerivedSharing.Compute do
  def count(%{items: items}) do
    [{_, derived_computes}] =
      :ets.lookup(Bench.SteadyStateDerivedSharing.Table, :derived_computes)

    Bench.InitialSharingSupport.bump(derived_computes)
    length(items)
  end
end

defmodule Bench.SteadyStateDerivedSharing do
  alias Bench.SteadyStateDerivedSharing.{Compute, Event, Source}
  alias Upkeep.Live

  @table Bench.SteadyStateDerivedSharing.Table

  def run(subscribers) do
    Bench.InitialSharingSupport.ensure_table(@table)

    parent = self()
    scope = System.unique_integer([:positive])
    source_loads = Bench.InitialSharingSupport.counter()
    derived_computes = Bench.InitialSharingSupport.counter()

    :ets.insert(@table, {:source_loads, source_loads})
    :ets.insert(@table, {:derived_computes, derived_computes})
    :ets.insert(@table, {{:value, scope}, [:before]})

    pids =
      for _ <- 1..subscribers do
        spawn_link(fn -> subscriber(parent, scope) end)
      end

    wait_for_ready(subscribers)

    Bench.InitialSharingSupport.counter_value(source_loads)
    Bench.InitialSharingSupport.counter_value(derived_computes)

    :counters.put(source_loads, 1, 0)
    :counters.put(derived_computes, 1, 0)
    :ets.insert(@table, {{:value, scope}, [:before, :after]})

    {update_us, deliveries} =
      Bench.InitialSharingSupport.timed(fn ->
        :ok =
          Event
          |> struct(%{scope: scope})
          |> Upkeep.Change.updated()
          |> Upkeep.notify()

        :ok = Upkeep.Coordinator.Graph.drain()
        wait_for_updates(subscribers)
      end)

    Enum.each(pids, fn pid -> send(pid, :stop) end)

    %{
      subscribers: subscribers,
      update_us: update_us,
      deliveries: deliveries,
      source_loads: Bench.InitialSharingSupport.counter_value(source_loads),
      derived_computes: Bench.InitialSharingSupport.counter_value(derived_computes)
    }
  end

  defp subscriber(parent, scope) do
    socket =
      Bench.InitialSharingSupport.socket()
      |> Live.watch(:items, Source, scope: scope)
      |> Live.derive(:count, [:items], &Compute.count/1)

    send(parent, {:ready, self()})
    loop(parent, socket)
  end

  defp loop(parent, socket) do
    receive do
      {:dag_values, pairs} ->
        socket = Live.apply_dag_values(socket, pairs)

        if socket.assigns.count != 2 do
          raise "subscriber received wrong count #{inspect(socket.assigns.count)}"
        end

        send(parent, {:updated, self()})
        loop(parent, socket)

      :stop ->
        :ok
    after
      10_000 ->
        raise "subscriber timed out waiting for graph update"
    end
  end

  defp wait_for_ready(count) do
    for _ <- 1..count do
      receive do
        {:ready, _pid} -> :ok
      after
        10_000 -> raise "subscribers did not finish mounting"
      end
    end
  end

  defp wait_for_updates(count) do
    for _ <- 1..count do
      receive do
        {:updated, _pid} -> :ok
      after
        10_000 -> raise "subscribers did not receive update"
      end
    end

    count
  end
end

subscribers = Bench.InitialSharingSupport.default_watches()
result = Bench.SteadyStateDerivedSharing.run(subscribers)

Bench.InitialSharingSupport.print_table(
  "steady_state_subscribers=#{result.subscribers}",
  ["source", "derived", "delivered"],
  [
    {"update", [result.source_loads, result.derived_computes, result.deliveries],
     result.update_us}
  ]
)

Bench.InitialSharingSupport.assert_equal!(
  result.source_loads,
  1,
  "steady-state source loaded #{result.source_loads} times"
)

Bench.InitialSharingSupport.assert_equal!(
  result.derived_computes,
  1,
  "steady-state shared derived computed #{result.derived_computes} times"
)

Bench.InitialSharingSupport.assert_equal!(
  result.deliveries,
  subscribers,
  "steady-state update delivered to #{result.deliveries} subscribers"
)

Bench.InitialSharingSupport.ok()
