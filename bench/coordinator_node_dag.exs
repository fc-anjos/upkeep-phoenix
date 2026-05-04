# Bench the centralized DAG prototype against the sharded LoadOnce variant.
#
#   mix run bench/coordinator_node_dag.exs
#
# Same workload model as coordinator_query_load.exs:
#   - 100 subscribers, 200-event pool, 50µs simulated query work
#   - subscribers receive pre-loaded values; no per-sub query
#
# We compare:
#   sharded_push   – Sharded.LoadOnce: indexed by group keys, dedup via flush.
#   node_dag_push  – NodeDAG: indexed by interest keys at register time;
#                    notify is a hash lookup + per-affected-node load.
#
# What to look for:
#   - notify cost: NodeDAG should win since no event_keys subset enumeration.
#   - queries/s: should match (both load once per affected node).
#   - throughput ceiling: NodeDAG is single-process; should hit a wall earlier.

Application.ensure_all_started(:upkeep)
{:ok, _} = Upkeep.Coordinator.ensure_started()
{:ok, _} = Upkeep.Coordinator.Sharded.LoadOnce.start_link()
{:ok, _} = Upkeep.Coordinator.NodeDAG.start_link()

defmodule Bench.Q do
  @query_us 50

  def busy_wait_us(us) do
    deadline = System.monotonic_time(:microsecond) + us
    loop_until(deadline)
  end

  defp loop_until(deadline) do
    if System.monotonic_time(:microsecond) >= deadline,
      do: :ok,
      else: loop_until(deadline)
  end

  def fake_query(_event), do: busy_wait_us(@query_us)
  def query_us, do: @query_us
end

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

defmodule Bench.Keys do
  @doc "Return the narrowest interest key matching exactly this event."
  def narrow(event) do
    fields = event |> Map.from_struct() |> Map.to_list() |> Enum.sort()
    {:upkeep_event, event.__struct__, fields}
  end
end

defmodule Bench.PushSub do
  @supervisor Upkeep.DurableSupervisor

  def spawn_for_groups(count, keys, deliveries) do
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

  def spawn_for_dag(count, pool, queries, deliveries) do
    parent = self()

    load_fn = fn ->
      :counters.add(queries, 1, 1)
      Bench.Q.fake_query(nil)
    end

    pids =
      for _ <- 1..count do
        spawn_link(fn ->
          Enum.each(pool, fn event ->
            node_id = {event.__struct__, event.id}
            interest_keys = [Bench.Keys.narrow(event)]
            Upkeep.Coordinator.NodeDAG.register_source(node_id, interest_keys, load_fn)
          end)

          send(parent, :ready)
          loop(deliveries)
        end)
      end

    for _ <- 1..count, do: receive do: (:ready -> :ok)
    pids
  end

  defp loop(d) do
    receive do
      {:upkeep_value, _e, _v} ->
        :counters.add(d, 1, 1)
        loop(d)

      {:dag_value, _n, _v} ->
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

group_keys =
  pool
  |> Enum.map(&Bench.Keys.narrow/1)
  |> Enum.uniq()
  |> Enum.map(&Upkeep.Source.group_key/1)

n_subs = 100
duration_ms = 3_000
publisher_levels = [1, 4, 8, 16]

IO.puts(
  "subscribers=#{n_subs} pool_size=#{length(pool)} group_keys=#{length(group_keys)} query_us=#{Bench.Q.query_us()}"
)

IO.puts(
  "\n#{String.pad_trailing("strategy", 16)} #{String.pad_trailing("pubs", 5)} #{String.pad_trailing("calls/s", 11)} #{String.pad_trailing("delivered/s", 13)} #{String.pad_trailing("queries/s", 11)} q/event"
)

reset = fn ->
  :counters.put(queries, 1, 0)
  :counters.put(deliveries, 1, 0)
end

print_row = fn name, p, calls, d, q, duration_ms ->
  calls_per_s = calls * 1_000 / duration_ms
  delivered_per_s = d * 1_000 / duration_ms
  queries_per_s = q * 1_000 / duration_ms
  q_per_event = if calls == 0, do: 0.0, else: q / calls

  IO.puts(
    [
      String.pad_trailing(name, 16),
      String.pad_trailing(Integer.to_string(p), 5),
      String.pad_trailing(:erlang.float_to_binary(calls_per_s, decimals: 0), 11),
      String.pad_trailing(:erlang.float_to_binary(delivered_per_s, decimals: 0), 13),
      String.pad_trailing(:erlang.float_to_binary(queries_per_s, decimals: 0), 11),
      :erlang.float_to_binary(q_per_event, decimals: 2)
    ]
    |> Enum.join(" ")
  )
end

run_sharded_push = fn ->
  for p <- publisher_levels do
    subs = Bench.PushSub.spawn_for_groups(n_subs, group_keys, deliveries)
    Process.sleep(150)
    reset.()
    calls = Bench.Run.run(&Upkeep.Coordinator.Sharded.LoadOnce.notify/1, p, duration_ms, pool)
    Upkeep.Coordinator.Sharded.LoadOnce.drain()
    Process.sleep(800)

    q = :counters.get(queries, 1)
    d = :counters.get(deliveries, 1)

    Enum.each(subs, fn pid ->
      Process.unlink(pid)
      Process.exit(pid, :kill)
    end)

    Process.sleep(50)

    print_row.("sharded_push", p, calls, d, q, duration_ms)
  end
end

run_node_dag = fn ->
  for p <- publisher_levels do
    subs = Bench.PushSub.spawn_for_dag(n_subs, pool, queries, deliveries)
    Process.sleep(150)
    reset.()
    calls = Bench.Run.run(&Upkeep.Coordinator.NodeDAG.notify/1, p, duration_ms, pool)
    Upkeep.Coordinator.NodeDAG.drain()
    Process.sleep(800)

    q = :counters.get(queries, 1)
    d = :counters.get(deliveries, 1)

    Enum.each(subs, fn pid ->
      Process.unlink(pid)
      Process.exit(pid, :kill)
    end)

    Process.sleep(150)
    print_row.("node_dag_push", p, calls, d, q, duration_ms)
  end
end

run_sharded_push.()
run_node_dag.()
