defmodule Upkeep.Runtime.Result do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Runtime.Telemetry
  alias Upkeep.Runtime.Subscriptions

  def to_socket({:ok, socket, effects}) when is_list(effects) do
    started_at = System.monotonic_time()
    socket = Enum.reduce(effects, socket, &apply_one/2)
    emit_effects_applied(effects, started_at)
    socket
  end

  defp apply_one({:assign, name, value}, socket) do
    assign(socket, name, value)
  end

  defp apply_one({:telemetry, event, measurements, metadata}, socket) do
    Telemetry.emit(event, measurements, metadata)
    socket
  end

  defp apply_one({:register_source, source_id, surface, instance}, socket) do
    Subscriptions.register(source_id, surface, instance)
    socket
  end

  defp apply_one({:register_derived, node_id, dep_node_ids, compute_fn}, socket) do
    :ok = Subscriptions.register_derived(node_id, dep_node_ids, compute_fn)
    socket
  end

  defp apply_one({:unregister, source_id}, socket) do
    Subscriptions.unregister(source_id)
    socket
  end

  defp emit_effects_applied(effects, started_at) do
    effect_counts = Enum.frequencies_by(effects, &effect_kind/1)

    :telemetry.execute(
      [:upkeep, :live, :effects, :apply],
      %{count: 1, duration: System.monotonic_time() - started_at},
      %{
        effect_count: length(effects),
        assign_count: Map.get(effect_counts, :assign, 0),
        telemetry_count: Map.get(effect_counts, :telemetry, 0),
        register_source_count: Map.get(effect_counts, :register_source, 0),
        register_derived_count: Map.get(effect_counts, :register_derived, 0),
        unregister_count: Map.get(effect_counts, :unregister, 0)
      }
    )
  end

  defp effect_kind({:assign, _name, _value}), do: :assign
  defp effect_kind({:unregister, _source_id}), do: :unregister
  defp effect_kind({:telemetry, _event, _measurements, _metadata}), do: :telemetry

  defp effect_kind({:register_source, _source_id, _surface, _instance}),
    do: :register_source

  defp effect_kind({:register_derived, _node_id, _dep_node_ids, _compute_fn}),
    do: :register_derived

  defp effect_kind(_effect), do: :unknown
end
