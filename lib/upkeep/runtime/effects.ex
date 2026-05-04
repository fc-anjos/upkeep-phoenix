defmodule Upkeep.Runtime.Effects do
  @moduledoc false

  def assign(name, value), do: {:assign, name, value}

  def telemetry(event, measurements, metadata),
    do: {:telemetry, event, measurements, metadata}

  def register_source(source_id, interest_keys, source, params),
    do: {:register_source, source_id, interest_keys, source, params}

  def register_derived(node_id, dep_node_ids, compute_fn),
    do: {:register_derived, node_id, dep_node_ids, compute_fn}

  def unregister(source_id), do: {:unregister, source_id}
end
