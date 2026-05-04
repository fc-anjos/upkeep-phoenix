defmodule Upkeep.Live.Assigns do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Live.{Ids, Telemetry}
  alias Upkeep.Runtime.State

  def assign_source_value(socket, assign_name, value, source_id) do
    Telemetry.emit([:live, :assign], %{count: 1}, %{
      assign: assign_name,
      node_id: Ids.source_node_id(source_id),
      source_id: source_id,
      kind: :source
    })

    assign(socket, assign_name, value)
  end

  def assign_derived_value(socket, assign_name, value, node_id) do
    Telemetry.emit([:live, :assign], %{count: 1}, %{
      assign: assign_name,
      node_id: node_id,
      kind: :derived
    })

    assign(socket, assign_name, value)
  end

  def assign_component_value(socket, _node_id, value) when is_map(value) do
    Enum.reduce(value, socket, fn
      {assign_name, assign_value}, socket when is_atom(assign_name) ->
        assign(socket, assign_name, assign_value)

      {_assign_name, _assign_value}, socket ->
        socket
    end)
  end

  def assign_component_value(socket, _node_id, _value), do: socket

  def assign_component_assign_values(socket, component_id, value) when is_map(value) do
    Enum.reduce(value, socket, fn
      {assign_name, assign_value}, socket when is_atom(assign_name) ->
        socket
        |> State.put_assign_node(
          assign_name,
          Ids.component_assign_node_id(component_id, assign_name)
        )
        |> assign(assign_name, assign_value)

      {_assign_name, _assign_value}, socket ->
        socket
    end)
  end

  def assign_component_assign_values(socket, _component_id, _value), do: socket
end
