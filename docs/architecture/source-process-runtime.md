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
- `Upkeep.Coordinator.DerivedProcess` owns one materialized derived identity
  when an existing `Live.derive/4` can be safely shared.

There is no shard runtime and no runtime switch.

## Derived Sharing

`Live.derive/4` remains the only public API. A derive is materialized in the
coordinator only when the existing runtime can prove a stable identity:

- The derive function is an external MFA, not a captured function.
- Every dependency maps to an explicit Upkeep graph identity: a source identity
  or an already-materialized derived identity.
- The derived graph identity includes the view, assign name, full dependency
  graph identities, and function identity.
- Each direct dependency already has more than one subscriber. Single-subscriber
  derives stay local, which avoids creating one-off derived processes for
  per-subscriber identities.

No source parameter names are interpreted specially. Identity-dependent content
is separated by the source identity terms that Upkeep already builds.

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
mix run bench/derived_process_sharing.exs
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

Current single-runtime results:

```text
source_runtime_live_mixed subscribers=400 projects=20 iterations=20 items=100

runtime          items      activity   prefs      p50_us     p95_us     p99_us
source_process   20         20         0          1139       1359       7034

source_runtime_contention iterations=40 slow_ms=25

runtime          slow       fast       p50_us     p95_us     p99_us
source_process   40         40         214        293        528

source_runtime_broad_invalidation sources=1000 iterations=5

runtime          loads      procs      p50_us     p95_us     p99_us
source_process   5000       0          22086      24677      24677

source_runtime_churn sources=1000 iterations=5

runtime          procs        reg_p50      reg_p95      unreg_p50    unreg_p95    total_p50    total_p95
source_process   0            59823        62860        54487        58257        114431       117004

source_runtime_idle_retention sources=250 cycles=5

ttl          loads      procs      p50_us     p95_us     p99_us
0            1250       0          27711      32123      32123
:infinity    1250       250        20433      25270      25270

derived_process_sharing subscribers=400 projects=20 iterations=20 items=100 compute_us=0

case         init_cmp     refresh_cmp  loads        src_procs    drv_procs    mount_ms     p50_us       p95_us       p99_us
shared       20           20           20           20           20           43.24        464          553          5147
local        400          400          20           20           0            8.58         333          383          399

derived_process_sharing subscribers=400 projects=20 iterations=20 items=100 compute_us=500

case         init_cmp     refresh_cmp  loads        src_procs    drv_procs    mount_ms     p50_us       p95_us       p99_us
shared       20           20           20           20           20           43.71        922          1038         4770
local        400          400          20           20           0            14.05        1198         1318         1354

derived_process_sharing subscribers=1000 projects=1 iterations=5 items=100 compute_us=10000

case         init_cmp     refresh_cmp  loads        src_procs    drv_procs    mount_ms     p50_us       p95_us       p99_us
shared       1            5            5            1            1            247.68       29981        32933        32933
local        1000         5000         5            1            0            201.65       41290        45469       45469
```
