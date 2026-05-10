# Graph throughput regression gate.
#
#   mix run bench/graph.exs
#
# Drives the production Graph with a realistic workload (100 subscribers,
# 200-event pool, 50µs simulated query work, narrow-key registration) at
# four publisher concurrency levels. Reports calls/s, deliveries/s,
# queries/s, q/event, then asserts minimum thresholds at the highest
# publisher count.
#
# Use as a merge gate: a regression in any of the assertion thresholds
# fails the bench. Numbers reflect Apple M2 (8 cores) — adjust thresholds
# for your CI hardware.

Application.ensure_all_started(:upkeep)

defmodule Bench.Telemetry do
  @table :bench_graph_telemetry
  @handler_id "bench-graph-telemetry"
  @events [
    [:upkeep, :graph, :invalidation],
    [:upkeep, :graph, :notifier, :flush],
    [:upkeep, :graph, :source_load, :stop],
    [:upkeep, :graph, :dispatch, :stop]
  ]
  @duration_buckets_us [
    1,
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000
  ]

  def attach do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      write_concurrency: true,
      read_concurrency: true
    ])

    :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, nil)
  end

  def detach do
    :telemetry.detach(@handler_id)
  end

  def handle_event(event, measurements, metadata, _config) do
    key = List.to_tuple(event)

    add(key, :count, Map.get(measurements, :count, 1))
    add_duration(key, measurements)

    metadata
    |> Map.take([
      :candidate_count,
      :matched_count,
      :candidate_key_count,
      :message_count,
      :event_count,
      :source_node_count,
      :shard_count,
      :pair_count,
      :pid_count,
      :subscriber_count
    ])
    |> Enum.each(fn {field, value} when is_integer(value) ->
      add(key, field, value)
    end)
  end

  def report do
    IO.puts("")
    IO.puts("Telemetry averages:")

    [
      {[:upkeep, :graph, :invalidation], "invalidation"},
      {[:upkeep, :graph, :notifier, :flush], "notifier_flush"},
      {[:upkeep, :graph, :source_load, :stop], "source_load"},
      {[:upkeep, :graph, :dispatch, :stop], "dispatch"}
    ]
    |> Enum.each(fn {event, label} ->
      key = List.to_tuple(event)
      count = read(key, :count)

      if count > 0 do
        duration = read(key, :duration)
        duration_count = read(key, :duration_count)
        avg_us = avg_duration_us(duration, duration_count)

        IO.puts(
          "  #{String.pad_trailing(label, 18)} count=#{count} avg_us=#{avg_us}" <>
            percentile_summary(key, duration_count) <>
            averages(key, count)
        )
      end
    end)
  end

  defp add_duration(key, %{duration: duration}) when is_integer(duration) do
    add(key, :duration, duration)
    add(key, :duration_count, 1)

    duration_us = System.convert_time_unit(duration, :native, :microsecond)
    add(key, {:duration_bucket_us, bucket_for(duration_us)}, 1)
  end

  defp add_duration(_key, _measurements), do: :ok

  defp averages(key, count) do
    [
      :candidate_key_count,
      :candidate_count,
      :matched_count,
      :message_count,
      :event_count,
      :source_node_count,
      :shard_count,
      :pair_count,
      :pid_count,
      :subscriber_count
    ]
    |> Enum.flat_map(fn field ->
      case read(key, field) do
        0 -> []
        value -> [" #{field}=#{Float.round(value / count, 2)}"]
      end
    end)
    |> Enum.join()
  end

  defp avg_duration_us(0, _count), do: "n/a"
  defp avg_duration_us(_duration, 0), do: "n/a"

  defp avg_duration_us(duration, count) do
    duration
    |> div(count)
    |> System.convert_time_unit(:native, :microsecond)
    |> Integer.to_string()
  end

  defp add(key, field, value) do
    :ets.update_counter(@table, {key, field}, {2, value}, {{key, field}, 0})
  end

  defp read(key, field) do
    case :ets.lookup(@table, {key, field}) do
      [{{^key, ^field}, value}] -> value
      [] -> 0
    end
  end

  defp percentile_summary(_key, 0), do: ""

  defp percentile_summary(key, duration_count) do
    " #{percentile_text(key, duration_count, 0.95, "p95_us")}" <>
      " #{percentile_text(key, duration_count, 0.99, "p99_us")}"
  end

  defp percentile_text(key, duration_count, percentile, label) do
    case percentile_bucket(key, duration_count, percentile) do
      {:bucket, bucket} -> "#{label}<=#{bucket}"
      {:over, bucket} -> "#{label}>#{bucket}"
    end
  end

  defp percentile_bucket(key, duration_count, percentile) do
    target = ceil(duration_count * percentile)

    result =
      @duration_buckets_us
      |> Enum.reduce_while(0, fn bucket, total ->
        total = total + read(key, {:duration_bucket_us, bucket})

        if total >= target do
          {:halt, {:bucket, bucket}}
        else
          {:cont, total}
        end
      end)

    case result do
      {:bucket, bucket} -> {:bucket, bucket}
      _total -> {:over, List.last(@duration_buckets_us)}
    end
  end

  defp bucket_for(duration_us) do
    Enum.find(@duration_buckets_us, ">#{List.last(@duration_buckets_us)}", &(duration_us <= &1))
  end
end

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

  def fake_query, do: busy_wait_us(@query_us)
  def query_us, do: @query_us
end

for mod <- [
      Bench.EvA,
      Bench.EvB,
      Bench.EvC,
      Bench.EvD,
      Bench.EvE,
      Bench.EvF,
      Bench.EvG,
      Bench.EvH
    ] do
  defmodule mod do
    defstruct [:id, :name, :schema, :tenant_id]
  end
end

defmodule Bench.Keys do
  def narrow(event) do
    fields = event |> Map.from_struct() |> Map.to_list() |> Enum.sort()
    {:upkeep_event, event.__struct__, fields}
  end

  def surface(event) do
    key = narrow(event)

    Upkeep.InvalidationSurface.manual([key], fn
      event when is_struct(event) -> narrow(event) == key
      _event -> false
    end)
  end
end

defmodule Bench.Sub do
  # Half the subs subscribe to source nodes; half subscribe to derived nodes
  # that auto-colocate with their source. This exercises multi-shard
  # source dispatch + multi-shard derived recompute simultaneously.
  def spawn_many(count, pool, queries, derived_computes, deliveries) do
    parent = self()
    half = div(count, 2)

    source_pids =
      for _ <- 1..(count - half) do
        spawn_link(fn ->
          Enum.each(pool, fn event ->
            node_id = {:src, event.__struct__, event.id}
            surface = Bench.Keys.surface(event)

            load_fn = fn ->
              :counters.add(queries, 1, 1)
              Bench.Q.fake_query()
              {1, surface}
            end

            Upkeep.Coordinator.Graph.register_loader(node_id, surface, load_fn)
          end)

          send(parent, :ready)
          loop(deliveries)
        end)
      end

    derived_pids =
      for _ <- 1..half do
        spawn_link(fn ->
          Enum.each(pool, fn event ->
            source_id = {:src, event.__struct__, event.id}
            derived_id = {:der, event.__struct__, event.id}
            surface = Bench.Keys.surface(event)

            load_fn = fn ->
              :counters.add(queries, 1, 1)
              Bench.Q.fake_query()
              {1, surface}
            end

            compute_fn = fn deps ->
              :counters.add(derived_computes, 1, 1)
              Map.fetch!(deps, source_id) * 2
            end

            Upkeep.Coordinator.Graph.register_loader(source_id, surface, load_fn)
            Upkeep.Coordinator.Graph.register_derived(derived_id, [source_id], compute_fn)
          end)

          send(parent, :ready)
          loop(deliveries)
        end)
      end

    for _ <- 1..count, do: receive(do: (:ready -> :ok))
    source_pids ++ derived_pids
  end

  def sync_all(pids) do
    ref = make_ref()

    Enum.each(pids, fn pid ->
      send(pid, {:sync, self(), ref})
    end)

    Enum.each(pids, fn pid ->
      receive do
        {:synced, ^ref, ^pid} -> :ok
      after
        5_000 -> raise "subscriber #{inspect(pid)} did not sync"
      end
    end)
  end

  defp loop(d) do
    receive do
      {:dag_values, pairs} ->
        :counters.add(d, 1, length(pairs))
        loop(d)

      {:sync, caller, ref} ->
        send(caller, {:synced, ref, self()})
        loop(d)

      _ ->
        loop(d)
    end
  end
end

defmodule Bench.Run do
  @event_modules [
    Bench.EvA,
    Bench.EvB,
    Bench.EvC,
    Bench.EvD,
    Bench.EvE,
    Bench.EvF,
    Bench.EvG,
    Bench.EvH
  ]

  def event_pool(unique_per_shape \\ 25) do
    for mod <- @event_modules,
        id <- 1..unique_per_shape,
        do: struct(mod, id: id, name: :insert, schema: :widgets, tenant_id: rem(id, 5))
  end

  def run(publishers, duration_ms, pool) do
    deadline = System.monotonic_time(:millisecond) + duration_ms
    pool_tuple = List.to_tuple(pool)
    pool_size = tuple_size(pool_tuple)

    tasks =
      for _ <- 1..publishers do
        Task.async(fn -> loop(pool_tuple, pool_size, deadline, 0) end)
      end

    counts = Enum.map(tasks, &Task.await(&1, duration_ms + 5_000))
    Enum.sum(counts)
  end

  defp loop(pool, size, deadline, n) do
    if System.monotonic_time(:millisecond) >= deadline do
      n
    else
      event = elem(pool, :rand.uniform(size) - 1)
      Upkeep.Invalidation.dispatch(event)
      loop(pool, size, deadline, n + 1)
    end
  end
end

queries = :counters.new(1, [:atomics])
derived_computes = :counters.new(1, [:atomics])
deliveries = :counters.new(1, [:atomics])
pool = Bench.Run.event_pool()

Bench.Telemetry.attach()

n_subs = 100
duration_ms = 3_000
publisher_levels = [1, 4, 8, 16]

subs = Bench.Sub.spawn_many(n_subs, pool, queries, derived_computes, deliveries)
# Drain shards so :group :joined backlog is processed before measuring.
Upkeep.Coordinator.Graph.drain()
Bench.Sub.sync_all(subs)

IO.puts(
  "subscribers=#{n_subs} (50 source + 50 derived) pool_size=#{length(pool)} query_us=#{Bench.Q.query_us()}"
)

IO.puts(
  "\n#{String.pad_trailing("pubs", 5)} #{String.pad_trailing("calls/s", 11)} #{String.pad_trailing("delivered/s", 13)} #{String.pad_trailing("queries/s", 11)} #{String.pad_trailing("derived/s", 11)} q/event"
)

results =
  for p <- publisher_levels do
    :counters.put(queries, 1, 0)
    :counters.put(derived_computes, 1, 0)
    :counters.put(deliveries, 1, 0)

    calls = Bench.Run.run(p, duration_ms, pool)
    Upkeep.Coordinator.Graph.drain()
    Bench.Sub.sync_all(subs)

    q = :counters.get(queries, 1)
    dc = :counters.get(derived_computes, 1)
    d = :counters.get(deliveries, 1)

    calls_per_s = calls * 1_000 / duration_ms
    delivered_per_s = d * 1_000 / duration_ms
    queries_per_s = q * 1_000 / duration_ms
    derived_per_s = dc * 1_000 / duration_ms
    q_per_event = if calls == 0, do: 0.0, else: q / calls

    IO.puts(
      [
        String.pad_trailing(Integer.to_string(p), 5),
        String.pad_trailing(:erlang.float_to_binary(calls_per_s, decimals: 0), 11),
        String.pad_trailing(:erlang.float_to_binary(delivered_per_s, decimals: 0), 13),
        String.pad_trailing(:erlang.float_to_binary(queries_per_s, decimals: 0), 11),
        String.pad_trailing(:erlang.float_to_binary(derived_per_s, decimals: 0), 11),
        :erlang.float_to_binary(q_per_event, decimals: 2)
      ]
      |> Enum.join(" ")
    )

    %{
      pubs: p,
      calls_per_s: calls_per_s,
      delivered_per_s: delivered_per_s,
      queries_per_s: queries_per_s,
      q_per_event: q_per_event,
      derived_per_s: derived_per_s
    }
  end

Bench.Telemetry.report()
Bench.Telemetry.detach()

# Regression gates: thresholds from the M2 baseline at 16 publishers.
# Tighten for production CI; the values below are conservative floors.
top = Enum.find(results, &(&1.pubs == 16))

thresholds = %{
  min_calls_per_s: 100_000,
  min_delivered_per_s: 20_000,
  max_q_per_event: 0.20
}

IO.puts("")
IO.puts("Gate (16 publishers):")

failures =
  []
  |> then(fn acc ->
    if top.calls_per_s < thresholds.min_calls_per_s,
      do: ["calls/s #{top.calls_per_s} < #{thresholds.min_calls_per_s}" | acc],
      else: acc
  end)
  |> then(fn acc ->
    if top.delivered_per_s < thresholds.min_delivered_per_s,
      do: ["delivered/s #{top.delivered_per_s} < #{thresholds.min_delivered_per_s}" | acc],
      else: acc
  end)
  |> then(fn acc ->
    if top.q_per_event > thresholds.max_q_per_event,
      do: ["q/event #{top.q_per_event} > #{thresholds.max_q_per_event}" | acc],
      else: acc
  end)

if failures == [] do
  IO.puts(
    "  OK — calls/s=#{Float.round(top.calls_per_s)} delivered/s=#{Float.round(top.delivered_per_s)} q/event=#{Float.round(top.q_per_event, 3)}"
  )

  System.halt(0)
else
  IO.puts("  FAIL")
  Enum.each(failures, &IO.puts("  - #{&1}"))
  System.halt(1)
end
