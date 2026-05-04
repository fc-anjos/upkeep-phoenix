defmodule Upkeep.Live.Subscriptions do
  @moduledoc false

  alias Upkeep.Coordinator.NodeDAG

  @doc """
  Register a watched source with the NodeDAG. The shared coordinator owns
  the interest index and runs `load_fn` once per affected node, fanning the
  resulting value out to all interested LVs.

  `load_fn` returns `{value, current_interest_keys}`. If subsequent loads
  produce a different key set (a source whose query branches on input
  data), the shard reconciles the index automatically — see
  `Upkeep.Coordinator.NodeDAG.Shard` for the reconciliation logic.
  """
  def register(source_id, interest_keys, source, params) do
    static_keys = source.__upkeep_interest_keys__(params)

    load_fn = fn ->
      {value, deps} = Upkeep.Source.load(source, params)
      dep_keys = Upkeep.Source.deps_interest_keys(deps)
      {value, Enum.uniq(static_keys ++ dep_keys)}
    end

    :ok = NodeDAG.register_source(source_id, interest_keys, load_fn)
  end

  def unregister(source_id) do
    NodeDAG.unregister(source_id)
  end

  def register_interest?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: true

  def register_interest?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)
end
