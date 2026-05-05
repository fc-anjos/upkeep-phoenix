defmodule Upkeep.Live.Components do
  @moduledoc false

  alias Upkeep.DAG.Store
  alias Upkeep.Live.Ids

  def component_node?({:component, _component_id}), do: true
  def component_node?(_node_id), do: false

  def changed_component_ids(node_ids) do
    node_ids
    |> Enum.filter(&component_node?/1)
    |> Enum.map(fn {:component, component_id} -> component_id end)
  end

  def put_assign_nodes(store, component_id, value) when is_map(value) do
    component_node_id = Ids.component_node_id(component_id)

    Enum.reduce(value, store, fn
      {assign_name, _assign_value}, store when is_atom(assign_name) ->
        assign_node_id = Ids.component_assign_node_id(component_id, assign_name)

        Store.put_derived(store, assign_node_id, [component_node_id], fn node_values ->
          node_values
          |> Map.fetch!(component_node_id)
          |> Map.fetch!(assign_name)
        end)

      {_assign_name, _assign_value}, store ->
        store
    end)
  end

  def put_assign_nodes(store, _component_id, _value), do: store
end
