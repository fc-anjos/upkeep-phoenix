defmodule Upkeep.Runtime.Subscriptions do
  @moduledoc false

  alias Upkeep.Internal.Coordinator.Graph

  def register(source_id, interest_keys, source, params) do
    :ok = Graph.register_source(source_id, interest_keys, source, params)
  end

  def register_and_load(source_id, interest_keys, source, params) do
    Graph.register_source_and_load(source_id, interest_keys, source, params)
  end

  def register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata) do
    Graph.register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata)
  end

  def register_derived(node_id, dep_node_ids, compute_fn) do
    Graph.register_derived(node_id, dep_node_ids, compute_fn)
  end

  def unregister(source_id) do
    Graph.unregister(source_id)
  end

  def register_interest?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: true

  def register_interest?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)

  def shared_initial_load?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: false

  def shared_initial_load?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)
end
