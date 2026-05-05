defmodule Upkeep.DAG.Diff do
  @moduledoc """
  Result of `Upkeep.DAG.Store.recompute/3`.

  Pure data describing the recompute outcome:

    * `selected_node_ids` — every node visited (downstream of `roots`),
      in topological order, including those skipped or that did not change.
    * `recomputed_node_ids` — nodes whose compute function ran.
    * `changed_node_ids` — recomputed nodes whose value actually moved.
    * `skipped_node_ids` / `boundaries` — excluded by the `:skip` option.

  Subgraph/connected-component analysis lives on `Upkeep.DAG.Plan` (returned
  from `Graph.applicable_subgraphs/3` and `Graph.subgraph_plan/2`); callers
  who want it for a recompute can compute it lazily from
  `selected_node_ids` via `Graph.subgraphs_for/2`.
  """

  @enforce_keys [
    :roots,
    :selected_node_ids,
    :boundaries,
    :changed_node_ids,
    :recomputed_node_ids,
    :skipped_node_ids
  ]

  defstruct roots: [],
            selected_node_ids: [],
            boundaries: [],
            changed_node_ids: [],
            recomputed_node_ids: [],
            skipped_node_ids: []
end
