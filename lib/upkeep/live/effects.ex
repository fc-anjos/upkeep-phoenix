defmodule Upkeep.Live.Effects do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Live.Telemetry
  alias Upkeep.Runtime.Subscriptions

  def apply(socket, effects) when is_list(effects) do
    Enum.reduce(effects, socket, &apply_one/2)
  end

  defp apply_one({:assign, name, value}, socket) do
    assign(socket, name, value)
  end

  defp apply_one({:telemetry, event, measurements, metadata}, socket) do
    Telemetry.emit(event, measurements, metadata)
    socket
  end

  defp apply_one({:register_source, source_id, interest_keys, source, params}, socket) do
    Subscriptions.register(source_id, interest_keys, source, params)
    socket
  end

  defp apply_one({:register_derived, node_id, dep_node_ids, compute_fn}, socket) do
    try do
      :ok = Subscriptions.register_derived(node_id, dep_node_ids, compute_fn)
    rescue
      ArgumentError -> :ok
    end

    socket
  end

  defp apply_one({:unregister, source_id}, socket) do
    Subscriptions.unregister(source_id)
    socket
  end
end
