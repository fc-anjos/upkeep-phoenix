defmodule Upkeep.Live.Ids do
  @moduledoc false

  alias Upkeep.Source.Instance

  def source_node_id(source_id), do: {:source, source_id}
  def derived_node_id(assign_name), do: {:derived, assign_name}
  def component_node_id(component_id), do: {:component, component_id}
  def scope_node_id(assign_name), do: {:scope, assign_name}

  def component_assign_node_id(component_id, assign_name),
    do: {:component_assign, component_id, assign_name}

  def source_deps(nil), do: []
  def source_deps(component), do: [component_node_id(component)]

  def scoped_source_id(%Instance{id: source_id}, nil), do: source_id

  def scoped_source_id(%Instance{id: source_id}, component) when not is_nil(component) do
    {:scoped, component, source_id}
  end
end
