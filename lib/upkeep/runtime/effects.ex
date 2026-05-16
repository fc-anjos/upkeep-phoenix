defmodule Upkeep.Runtime.Effects do
  @moduledoc false

  alias Upkeep.Runtime.Ids

  def maybe_register_source(true, source_id, surface, producer) do
    instance = producer.instance
    [{:register_source, source_id, surface, instance}]
  end

  def maybe_register_source(false, _source_id, _surface, _producer), do: []

  def assign_source(assign_name, value, source_id) do
    [
      {:telemetry, [:live, :assign], %{count: 1},
       %{
         assign: assign_name,
         node_id: Ids.source_node_id(source_id),
         source_id: source_id,
         kind: :source
       }},
      {:assign, assign_name, value}
    ]
  end

  def assign_derived(assign_name, value, node_id) do
    [
      {:telemetry, [:live, :assign], %{count: 1},
       %{
         assign: assign_name,
         node_id: node_id,
         kind: :derived
       }},
      {:assign, assign_name, value}
    ]
  end

  def component_assigns(value) when is_map(value) do
    value
    |> Enum.flat_map(fn
      {assign_name, assign_value} when is_atom(assign_name) ->
        [{:assign, assign_name, assign_value}]

      {_assign_name, _assign_value} ->
        []
    end)
  end

  def component_assigns(_value), do: []

  def assign_watch(watch, value) do
    Enum.flat_map(watch.assign_names, fn assign_name ->
      assign_source(assign_name, value, watch.source_id)
    end)
  end
end
