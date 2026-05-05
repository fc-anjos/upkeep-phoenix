defmodule Upkeep.DAG.Diff do
  @moduledoc """
  Result of `Upkeep.DAG.Store.recompute/3`.

  Describes both the topological region considered (`selected_node_ids`,
  `subgraphs`) and the dynamic outcome (`recomputed_node_ids` ran the compute
  function, `changed_node_ids` actually produced a new value, `skipped_node_ids`
  were excluded by `:skip`). Pure data — safe to telemetry, snapshot, or ship
  over the wire.
  """

  @enforce_keys [
    :roots,
    :selected_node_ids,
    :subgraphs,
    :largest_subgraphs,
    :boundaries,
    :changed_node_ids,
    :recomputed_node_ids,
    :skipped_node_ids
  ]

  defstruct roots: [],
            selected_node_ids: [],
            subgraphs: [],
            largest_subgraphs: [],
            boundaries: [],
            changed_node_ids: [],
            recomputed_node_ids: [],
            skipped_node_ids: []
end
