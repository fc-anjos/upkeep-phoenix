defmodule Upkeep.Live.Subscriptions do
  @moduledoc false

  alias Upkeep.Coordinator.NodeDAG

  @doc """
  Register a watched source with the NodeDAG. The shared coordinator owns
  the interest index and runs `load_fn` once per affected node, fanning the
  resulting value out to all interested LVs.
  """
  def register(source_id, interest_keys, source, params) do
    load_fn = fn -> source.load(params) end
    :ok = NodeDAG.register_source(source_id, interest_keys, load_fn)
  end

  def unregister(source_id) do
    NodeDAG.unregister(source_id)
  end

  def register_interest?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: true

  def register_interest?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)
end
