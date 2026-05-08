defmodule Upkeep.DAG.Plan do
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

  @type node_id :: term()
  @type subgraph :: %{node_ids: [node_id()], count: non_neg_integer()}
  @type boundary :: %{node_id: node_id(), reason: term()}
  @type t :: %__MODULE__{
          roots: [node_id()],
          selected_node_ids: [node_id()],
          subgraphs: [subgraph()],
          largest_subgraphs: [subgraph()],
          boundaries: [boundary()]
        }
end
