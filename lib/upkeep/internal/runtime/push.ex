defmodule Upkeep.Internal.Runtime.Push do
  @moduledoc false

  alias Upkeep.Live.Ids
  alias Upkeep.Internal.Runtime.DAGOperations
  alias Upkeep.Internal.Runtime.Effects
  alias Upkeep.Internal.Runtime.State

  def apply_dag_values(socket, pairs) when is_list(pairs) do
    {socket, changed_nodes, shared_nodes, effects} =
      Enum.reduce(pairs, {socket, [], [], []}, fn {node_id, value},
                                                  {socket, changed, shared, effects} ->
        case put_pushed_value(socket, node_id, value) do
          {:source, socket, local_node_id, true, assign_effects} ->
            {socket, [local_node_id | changed], shared, effects ++ assign_effects}

          {:source, socket, _local_node_id, false, assign_effects} ->
            {socket, changed, shared, effects ++ assign_effects}

          {:shared, socket, local_node_id, true, assign_effects} ->
            {socket, [local_node_id | changed], [local_node_id | shared],
             effects ++ assign_effects}

          {:shared, socket, local_node_id, false, assign_effects} ->
            {socket, changed, [local_node_id | shared], effects ++ assign_effects}

          :unknown ->
            {socket, changed, shared, effects}
        end
      end)

    {socket, recompute_effects} =
      Upkeep.Internal.Runtime.recompute_derived(socket, changed_nodes, skip: shared_nodes)

    {:ok, socket, effects ++ recompute_effects}
  end

  def apply_dag_value(socket, source_id, value) do
    case put_pushed_value(socket, source_id, value) do
      {_kind, socket, local_node_id, true, assign_effects} ->
        {socket, recompute_effects} =
          Upkeep.Internal.Runtime.recompute_derived(socket, [local_node_id])

        {:ok, socket, assign_effects ++ recompute_effects}

      {_kind, socket, _local_node_id, false, assign_effects} ->
        {:ok, socket, assign_effects}

      :unknown ->
        {:ok, socket, []}
    end
  end

  defp put_pushed_value(socket, source_id, value) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        {socket, changed?} =
          DAGOperations.put_value(socket, source_id, value, Ids.source_deps(watch.component))

        {:source, socket, Ids.source_node_id(source_id), changed?,
         Effects.assign_watch(watch, value)}

      :error ->
        put_pushed_shared_value(socket, source_id, value)
    end
  end

  defp put_pushed_shared_value(socket, graph_node_id, value) do
    case State.local_shared_derived_node(socket, graph_node_id) do
      nil ->
        :unknown

      local_node_id ->
        {socket, changed?} = DAGOperations.put_derived_value(socket, local_node_id, value)

        {:shared, socket, local_node_id, changed?,
         Effects.assign_shared_derived(socket, local_node_id, value)}
    end
  end
end
