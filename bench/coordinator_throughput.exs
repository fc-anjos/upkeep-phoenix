# Throughput bench: end-to-end fan-out with subscriber-side cost sampling.
#
#   mix run bench/coordinator_throughput.exs
#
# For each strategy and each publisher concurrency level, run for a fixed
# wall-clock window. Reports:
#   - publisher calls/s        (publisher throughput)
#   - delivered messages/s     (subscriber inbound rate)
#   - reductions/s per sub     (subscriber CPU cost)
#   - max mailbox at end       (queue buildup)

Application.ensure_all_started(:upkeep)
{:ok, _} = Upkeep.Coordinator.ensure_started()
{:ok, _} = Upkeep.Coordinator.Cast.start_link()
{:ok, _} = Upkeep.Coordinator.Sharded.start_link()

for mod <- [Bench.EvA, Bench.EvB, Bench.EvC, Bench.EvD, Bench.EvE, Bench.EvF, Bench.EvG, Bench.EvH] do
  defmodule mod do
    defstruct [:id, :name, :schema, :tenant_id]
  end
end

defmodule Bench.CountingSubscriber do
  @supervisor Upkeep.DurableSupervisor

  def spawn_many(count, keys) do
    parent = self()

    pids =
      for _ <- 1..count do
        spawn_link(fn ->
          Enum.each(keys, &Group.join(@supervisor, &1, %{kind: :bench}))
          send(parent, :ready)
          loop(0)
        end)
      end

    for _ <- 1..count, do: receive do: (:ready -> :ok)
    pids
  end

  def snapshot(pid) do
    ref = make_ref()
    send(pid, {:report, self(), ref})

    info = Process.info(pid, [:reductions, :message_queue_len])

    receive do
      {^ref, n} -> {n, info}
    after
      2_000 -> {:error, info}
    end
  end

  defp loop(n) do
    receive do
      {:report, from, ref} ->
        send(from, {ref, n})
        loop(n)

      _ ->
        loop(n + 1)
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

# Setup — subscribers join the union of interest keys across the whole pool.
pool = Bench.Run.event_pool()

keys =
  pool
  |> Enum.flat_map(&Upkeep.Source.event_keys/1)
  |> Enum.uniq()
  |> Enum.map(&Upkeep.Source.group_key/1)

n_subs = 100
subs = Bench.CountingSubscriber.spawn_many(n_subs, keys)
Process.sleep(200)

IO.puts("subscribers=#{n_subs} pool_size=#{length(pool)} total_keys=#{length(keys)}")

strategies = [
  {"stateless", &Upkeep.Coordinator.Stateless.notify/1},
  {"durable_call", &Upkeep.Coordinator.notify/1},
  {"plain_cast", &Upkeep.Coordinator.Cast.notify/1},
  {"sharded", &Upkeep.Coordinator.Sharded.notify/1}
]

publisher_levels = [1, 4, 8, 16]
duration_ms = 3_000

header =
  [
    String.pad_trailing("strategy", 14),
    String.pad_trailing("pubs", 5),
    String.pad_trailing("calls/s", 11),
    String.pad_trailing("delivered/s", 13),
    String.pad_trailing("red/s/sub", 12),
    String.pad_trailing("max_mbox", 9)
  ]
  |> Enum.join(" ")

IO.puts("\n" <> header)

for {name, fun} <- strategies, p <- publisher_levels do
  # Snapshot baseline counters/reductions.
  baseline =
    for sub <- subs, into: %{} do
      {n, info} = Bench.CountingSubscriber.snapshot(sub)
      {sub, {n, info[:reductions]}}
    end

  calls = Bench.Run.run(fun, p, duration_ms, pool)
  if name == "sharded", do: Upkeep.Coordinator.Sharded.drain()
  Process.sleep(500)

  finals =
    for sub <- subs, into: %{} do
      {n, info} = Bench.CountingSubscriber.snapshot(sub)
      {sub, {n, info[:reductions], info[:message_queue_len]}}
    end

  delivered =
    Enum.reduce(subs, 0, fn sub, acc ->
      {n0, _} = Map.fetch!(baseline, sub)
      {n1, _, _} = Map.fetch!(finals, sub)
      acc + (n1 - n0)
    end)

  reductions =
    Enum.reduce(subs, 0, fn sub, acc ->
      {_, r0} = Map.fetch!(baseline, sub)
      {_, r1, _} = Map.fetch!(finals, sub)
      acc + (r1 - r0)
    end)

  max_mbox =
    finals |> Map.values() |> Enum.map(fn {_, _, mq} -> mq end) |> Enum.max()

  calls_per_s = calls * 1_000 / duration_ms
  delivered_per_s = delivered * 1_000 / duration_ms
  red_per_s_per_sub = reductions / duration_ms / n_subs * 1_000

  IO.puts(
    [
      String.pad_trailing(name, 14),
      String.pad_trailing(Integer.to_string(p), 5),
      String.pad_trailing(:erlang.float_to_binary(calls_per_s, decimals: 0), 11),
      String.pad_trailing(:erlang.float_to_binary(delivered_per_s, decimals: 0), 13),
      String.pad_trailing(:erlang.float_to_binary(red_per_s_per_sub, decimals: 0), 12),
      String.pad_trailing(Integer.to_string(max_mbox), 9)
    ]
    |> Enum.join(" ")
  )
end
