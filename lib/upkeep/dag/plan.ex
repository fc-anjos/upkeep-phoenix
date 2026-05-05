defmodule Upkeep.DAG.Plan do
  @moduledoc """
  Describes a topologically-ordered region of a graph for a caller policy.

  Returned by `Upkeep.DAG.Graph` query functions like `subgraph_plan/2` and
  `applicable_subgraphs/3`. Pure data: roots and boundaries explain the
  decision, while selected nodes and subgraphs give callers a stable unit
  they can cache, invalidate, or report on. Recompute-time outcomes (changed,
  recomputed, skipped) live on `Upkeep.DAG.Diff` instead.
  """

  @enforce_keys [
    :roots,
    :selected_node_ids,
    :subgraphs,
    :largest_subgraphs,
    :boundaries
  ]

  defstruct roots: [],
            selected_node_ids: [],
            subgraphs: [],
            largest_subgraphs: [],
            boundaries: []
end
