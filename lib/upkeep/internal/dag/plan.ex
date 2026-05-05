defmodule Upkeep.Internal.DAG.Plan do
  @moduledoc false

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
