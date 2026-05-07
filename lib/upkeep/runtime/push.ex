defmodule Upkeep.Runtime.Push do
  @moduledoc false

  alias Upkeep.Live.Ids
  alias Upkeep.Runtime.DAGOperations
  alias Upkeep.Runtime.Effects
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Watches

  def apply_dag_values(socket, pairs) when is_list(pairs) do
    started_at = System.monotonic_time()

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
      recompute_derived(socket, changed_nodes, skip: shared_nodes)

    effects = effects ++ recompute_effects

    emit_apply_dag_values(
      pairs,
      changed_nodes,
      shared_nodes,
      effects,
      recompute_effects,
      started_at
    )

    {:ok, socket, effects}
  end

  def apply_dag_value(socket, source_id, value) do
    case put_pushed_value(socket, source_id, value) do
      {_kind, socket, local_node_id, true, assign_effects} ->
        {socket, recompute_effects} = recompute_derived(socket, [local_node_id])
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

  defp recompute_derived(socket, changed_source_nodes, opts \\ [])

  defp recompute_derived(socket, [], _opts), do: {socket, []}

  defp recompute_derived(socket, changed_source_nodes, opts) do
    DAGOperations.recompute_derived(socket, changed_source_nodes, &Watches.remove_watch/2, opts)
  end

  defp emit_apply_dag_values(
         pairs,
         changed_nodes,
         shared_nodes,
         effects,
         recompute_effects,
         started_at
       ) do
    :telemetry.execute(
      [:upkeep, :live, :dag_values, :apply],
      %{count: 1, duration: System.monotonic_time() - started_at},
      %{
        pair_count: length(pairs),
        changed_node_count: length(changed_nodes),
        shared_node_count: length(shared_nodes),
        effect_count: length(effects),
        assign_effect_count: Enum.count(effects, &match?({:assign, _name, _value}, &1)),
        recompute_effect_count: length(recompute_effects)
      }
    )
  end
end
