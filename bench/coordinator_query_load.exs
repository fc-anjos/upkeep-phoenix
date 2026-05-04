# Query-load bench: the headline metric for option 3 (load-once-fan-out-many).
#
#   mix run bench/coordinator_query_load.exs
#
# Each strategy delivers events to N subscribers. Subscribers do
# representative "query work" (50µs busy-wait simulating an indexed DB
# read) on each event they handle. We track total query-work executions
# system-wide.
#
# Strategies:
#   stateless_pull  – stateless dispatch; every subscriber queries on each event.
#   sharded_pull    – sharded coordinator with coalescing; subscribers query.
#   sharded_push    – Sharded.LoadOnce; the shard runs the query ONCE per
#                     coalesced event and pushes the value to subscribers.
#                     Subscribers do no query work.

Application.ensure_all_started(:upkeep)
{:ok, _} = Upkeep.Coordinator.ensure_started()
{:ok, _} = Upkeep.Coordinator.Sharded.start_link()
{:ok, _} = Upkeep.Coordinator.Sharded.LoadOnce.start_link()

defmodule Bench.Q do
  @moduledoc false

  @query_us 50

  def busy_wait_us(us) do
    deadline = System.monotonic_time(:microsecond) + us
    loop_until(deadline)
  end

  defp loop_until(deadline) do
    if System.monotonic_time(:microsecond) >= deadline do
      :ok
    else
      loop_until(deadline)
    end
  end

  def fake_query(_event) do
    busy_wait_us(@query_us)
    :loaded
  end

  def query_us, do: @query_us
end

# Counters: shared across processes
queries = :counters.new(1, [:atomics])
deliveries = :counters.new(1, [:atomics])

Upkeep.Coordinator.Sharded.LoadOnce.register_load_fn(fn event ->
  :counters.add(queries, 1, 1)
  Bench.Q.fake_query(event)
end)

for mod <- [Bench.EvA, Bench.EvB, Bench.EvC, Bench.EvD, Bench.EvE, Bench.EvF, Bench.EvG, Bench.EvH] do
  defmodule mod do
    defstruct [:id, :name, :schema, :tenant_id]
  end
end

defmodule Bench.PullSub do
  @moduledoc "Subscriber that queries on each event message."
  @supervisor Upkeep.DurableSupervisor

  def spawn_many(count, keys, queries, deliveries) do
    parent = self()

    pids =
      for _ <- 1..count do
        spawn_link(fn ->
          Enum.each(keys, &Group.join(@supervisor, &1, %{kind: :pull}))
          send(parent, :ready)
          loop(queries, deliveries)
        end)
      end

    for _ <- 1..count, do: receive do: (:ready -> :ok)
    pids
  end

  defp loop(q, d) do
    receive do
      {:upkeep_event, _event} ->
        :counters.add(d, 1, 1)
        :counters.add(q, 1, 1)
        Bench.Q.fake_query(nil)
        loop(q, d)

      {:upkeep_value, _event, _value} ->
        # tolerate but ignore — shouldn't happen for pull strategies
        :counters.add(d, 1, 1)
        loop(q, d)

      _ ->
        loop(q, d)
    end
  end
end

defmodule Bench.PushSub do
  @moduledoc "Subscriber that just records receipt; coordinator already ran the query."
  @supervisor Upkeep.DurableSupervisor

  def spawn_many(count, keys, deliveries) do
    parent = self()

    pids =
      for _ <- 1..count do
        spawn_link(fn ->
          Enum.each(keys, &Group.join(@supervisor, &1, %{kind: :push}))
          send(parent, :ready)
          loop(deliveries)
        end)
      end

    for _ <- 1..count, do: receive do: (:ready -> :ok)
    pids
  end

  defp loop(d) do
    receive do
      {:upkeep_value, _event, _value} ->
        :counters.add(d, 1, 1)
        loop(d)

      _ ->
        loop(d)
    end
  end
end

defmodule Bench.Run do
  @event_modules [Bench.EvA, Bench.EvB, Bench.EvC, Bench.EvD, Bench.EvE, Bench.EvF, Bench.EvG, Bench.EvH]

  def event_pool(unique_per_shape \\ 25) do
    for mod <- @event_modules,
        id <- 1..unique_per_shape,
        do: struct(mod, id: id, name: :insert, schema: :widgets, tenant_id: rem(id, 5))
  end

  def run(strategy_fun, publishers, duration_ms, pool) do
    deadline = System.monotonic_time(:millisecond) + duration_ms
    pool_tuple = List.to_tuple(pool)
    pool_size = tuple_size(pool_tuple)

    tasks =
      for _ <- 1..publishers do
        Task.async(fn -> loop(strategy_fun, pool_tuple, pool_size, deadline, 0) end)
      end

    counts = Enum.map(tasks, &Task.await(&1, duration_ms + 5_000))
    Enum.sum(counts)
  end

  defp loop(fun, pool, size, deadline, n) do
    if System.monotonic_time(:millisecond) >= deadline do
      n
    else
      event = elem(pool, :rand.uniform(size) - 1)
      fun.(event)
      loop(fun, pool, size, deadline, n + 1)
    end
  end
end

pool = Bench.Run.event_pool()

keys =
  pool
  |> Enum.flat_map(&Upkeep.Source.event_keys/1)
  |> Enum.uniq()
  |> Enum.map(&Upkeep.Source.group_key/1)

n_subs = 100
duration_ms = 3_000
publisher_levels = [1, 4, 8, 16]

IO.puts(
  "subscribers=#{n_subs} pool_size=#{length(pool)} keys=#{length(keys)} query_us=#{Bench.Q.query_us()}"
)

IO.puts(
  "\n#{String.pad_trailing("strategy", 18)} #{String.pad_trailing("pubs", 5)} #{String.pad_trailing("calls/s", 11)} #{String.pad_trailing("delivered/s", 13)} #{String.pad_trailing("queries/s", 11)} q/event"
)

reset_counters = fn ->
  :counters.put(queries, 1, 0)
  :counters.put(deliveries, 1, 0)
end

run_with_subs = fn strategy_name, sub_kind, fun ->
  for p <- publisher_levels do
    # Spawn fresh subscribers each run so we can kill them after.
    subs =
      case sub_kind do
        :pull -> Bench.PullSub.spawn_many(n_subs, keys, queries, deliveries)
        :push -> Bench.PushSub.spawn_many(n_subs, keys, deliveries)
      end

    Process.sleep(150)
    reset_counters.()
    calls = Bench.Run.run(fun, p, duration_ms, pool)

    case strategy_name do
      "sharded_pull" -> Upkeep.Coordinator.Sharded.drain()
      "sharded_push" -> Upkeep.Coordinator.Sharded.LoadOnce.drain()
      _ -> :ok
    end

    Process.sleep(800)
    q = :counters.get(queries, 1)
    d = :counters.get(deliveries, 1)

    Enum.each(subs, fn pid ->
      Process.unlink(pid)
      Process.exit(pid, :kill)
    end)

    Process.sleep(50)

    calls_per_s = calls * 1_000 / duration_ms
    delivered_per_s = d * 1_000 / duration_ms
    queries_per_s = q * 1_000 / duration_ms
    q_per_event = if calls == 0, do: 0.0, else: q / calls

    IO.puts(
      [
        String.pad_trailing(strategy_name, 18),
        String.pad_trailing(Integer.to_string(p), 5),
        String.pad_trailing(:erlang.float_to_binary(calls_per_s, decimals: 0), 11),
        String.pad_trailing(:erlang.float_to_binary(delivered_per_s, decimals: 0), 13),
        String.pad_trailing(:erlang.float_to_binary(queries_per_s, decimals: 0), 11),
        :erlang.float_to_binary(q_per_event, decimals: 2)
      ]
      |> Enum.join(" ")
    )
  end
end

run_with_subs.("stateless_pull", :pull, &Upkeep.Coordinator.Stateless.notify/1)
run_with_subs.("sharded_pull", :pull, &Upkeep.Coordinator.Sharded.notify/1)
run_with_subs.("sharded_push", :push, &Upkeep.Coordinator.Sharded.LoadOnce.notify/1)
