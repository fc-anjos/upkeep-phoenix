defmodule Upkeep.Internal.DAG.Diff do
  @moduledoc false

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
