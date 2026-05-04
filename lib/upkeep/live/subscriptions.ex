defmodule Upkeep.Live.Subscriptions do
  @moduledoc false

  alias Upkeep.Coordinator.Graph

  @doc """
  Register a watched source with the Graph. The shared coordinator owns
  the interest index and loads the source once per affected node, fanning
  the resulting value out to all interested LVs.
  """
  def register(source_id, interest_keys, source, params) do
    :ok = Graph.register_source(source_id, interest_keys, source, params)
  end

  def register_and_load(source_id, interest_keys, source, params) do
    Graph.register_source_and_load(source_id, interest_keys, source, params)
  end

  def register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata) do
    Graph.register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata)
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
