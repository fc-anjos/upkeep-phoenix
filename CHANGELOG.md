# Changelog

## v0.1.0

Initial public release.

- `Upkeep.Ecto.Repo` capture of committed inserts, updates, deletes, bulk
  writes, transactions, and `Ecto.Multi` operations.
- `Upkeep.Ecto.Source` with query-derived invalidation surfaces;
  `Upkeep.Source` with explicit `invalidated_by` surfaces for non-Ecto reads.
- `Upkeep.Live` with `watch`, `derive`, and `component` for LiveView assigns.
- Shared source processes with idle retention and materialized derive sharing.
- `Upkeep.Test` helpers, `Upkeep.attach_write_guard/2`, and `mix upkeep.audit`
  for catching out-of-band writes.
