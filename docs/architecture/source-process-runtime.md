# Source Process Runtime

Status: implemented as the only coordinator source runtime.

Each shared source identity is an independent supervised process:

- `Upkeep.Coordinator.SourceProcess` owns one source node, its cached value,
  surface, retry state, stale state, and idle timer.
- `Upkeep.Coordinator.SourceProcesses` owns the registry, dynamic supervisor,
  and source-load task supervisor.
- `Upkeep.Coordinator.Topology` is now only the ETS invalidation index and
  generation table.
- `Upkeep.Coordinator.Graph.Notifier` maps invalidation events to affected
  source ids and sends them directly to source processes.

There is no shard runtime and no runtime switch. LiveView derives are local DAG
nodes; expensive shared computations should be modeled as explicit sources
until a separate materialized-derived design is justified.

## Idle Retention

Source processes use identity-owned idle retention rather than a worker pool.

Retention is gated:

- If a source never had more than one concurrent subscriber, it stops
  immediately when the last subscriber leaves.
- If a source had more than one concurrent subscriber and has loaded a value, it
  can enter dormant retention.
- Idle invalidations mark the source stale but do not reload without
  subscribers.
- Reuse cancels the idle timer.
- The idle timer stops the process and unregisters its topology/index entry.

The default idle TTL comes from `config :upkeep, :source_idle_ttl_ms`. A source
can override it:

```elixir
defmodule MyApp.Sources.ProjectSummary do
  use Upkeep.Source, idle_ttl_ms: 120_000
end
```

Accepted values are:

- `nil` - use the app default.
- `0` - stop immediately.
- non-negative integer - retain for that many milliseconds.
- `:infinity` - retain until invalidated/reset/manual stop.

## Benchmark Scripts

Current source-runtime benchmarks:

```sh
mix run bench/source_runtime_contention.exs
mix run bench/source_runtime_broad_invalidation.exs
mix run bench/source_runtime_churn.exs
mix run bench/source_runtime_idle_retention.exs
mix run bench/source_runtime_live_mixed.exs
```

The decision benchmark before removing the shard path showed the useful shape:

```text
source_runtime_live_mixed subscribers=400 projects=20 iterations=20 items=100

runtime    items      activity   prefs      p50_us     p95_us     p99_us
shard      20         20         0          1017       1158       8022
process    20         20         0          1009       1208       1314
```

The main expected tradeoff remains lifecycle churn: one-off identities pay
process startup/teardown cost. Structural retention keeps that cost off the
shared-source path without retaining single-subscriber sources.

Current single-runtime local results:

```text
source_runtime_live_mixed subscribers=400 projects=20 iterations=20 items=100

runtime          items      activity   prefs      p50_us     p95_us     p99_us
source_process   20         20         0          922        1283       6965

source_runtime_contention iterations=40 slow_ms=25

runtime          slow       fast       p50_us     p95_us     p99_us
source_process   40         40         132        188        744

source_runtime_broad_invalidation sources=1000 iterations=5

runtime          loads      procs      p50_us     p95_us     p99_us
source_process   5000       0          23442      28628      28628

source_runtime_churn sources=1000 iterations=5

runtime          procs        reg_p50      reg_p95      unreg_p50    unreg_p95    total_p50    total_p95
source_process   0            61679        65722        53605        56300        116806       117979

source_runtime_idle_retention sources=250 cycles=5

ttl          loads      procs      p50_us     p95_us     p99_us
0            1250       0          30990      34706      34706
:infinity    1250       250        22439      26528      26528
```
