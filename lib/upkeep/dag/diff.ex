defmodule Upkeep.DAG.Diff do
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

  @type node_id :: term()
  @type boundary :: %{node_id: node_id(), reason: term()}
  @type t :: %__MODULE__{
          roots: [node_id()],
          selected_node_ids: [node_id()],
          boundaries: [boundary()],
          changed_node_ids: [node_id()],
          recomputed_node_ids: [node_id()],
          skipped_node_ids: [node_id()]
        }
end
