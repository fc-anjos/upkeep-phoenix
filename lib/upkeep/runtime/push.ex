defmodule Upkeep.Runtime.Push do
  @moduledoc false

  alias Upkeep.Live.Ids
  alias Upkeep.Runtime.DAGOperations
  alias Upkeep.Runtime.Effects
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Watches

  def apply_dag_values(socket, pairs) when is_list(pairs) do
    started_at = System.monotonic_time()

    {socket, changed_nodes, shared_nodes, effects, ignored} =
      Enum.reduce(pairs, {socket, [], [], [], []}, fn {node_id, value},
                                                      {socket, changed, shared, effects, ignored} ->
        case put_pushed_value(socket, node_id, value) do
          {:source, socket, local_node_id, true, assign_effects} ->
            {socket, [local_node_id | changed], shared, effects ++ assign_effects, ignored}

          {:source, socket, _local_node_id, false, assign_effects} ->
            {socket, changed, shared, effects ++ assign_effects, ignored}

          {:shared, socket, local_node_id, true, assign_effects} ->
            {socket, [local_node_id | changed], [local_node_id | shared],
             effects ++ assign_effects, ignored}

          {:shared, socket, local_node_id, false, assign_effects} ->
            {socket, changed, [local_node_id | shared], effects ++ assign_effects, ignored}

          {:ignored, reason, node_id} ->
            {socket, changed, shared, effects, [%{reason: reason, node_id: node_id} | ignored]}
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
      ignored,
      started_at
    )

    emit_ignored_dag_values(ignored)

    {:ok, socket, effects}
  end

  def apply_dag_value(socket, source_id, value) do
    case put_pushed_value(socket, source_id, value) do
      {_kind, socket, local_node_id, true, assign_effects} ->
        {socket, recompute_effects} = recompute_derived(socket, [local_node_id])
        {:ok, socket, assign_effects ++ recompute_effects}

      {_kind, socket, _local_node_id, false, assign_effects} ->
        {:ok, socket, assign_effects}

      {:ignored, reason, node_id} ->
        emit_ignored_dag_values([%{reason: reason, node_id: node_id}])
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
        {:ignored, ignored_reason(graph_node_id), graph_node_id}

      local_node_id ->
        {socket, changed?} = DAGOperations.put_derived_value(socket, local_node_id, value)

        {:shared, socket, local_node_id, changed?,
         Effects.assign_shared_derived(socket, local_node_id, value)}
    end
  end

  defp ignored_reason({_source, params}) when is_map(params), do: :unwatched_source

  defp ignored_reason({:scoped, _component, {_source, params}}) when is_map(params),
    do: :unwatched_source

  defp ignored_reason({:derived, _view, _assign_name, _deps, _compute_fn}),
    do: :unknown_shared_derived

  defp ignored_reason(_node_id), do: :stale_push

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
         ignored,
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
        recompute_effect_count: length(recompute_effects),
        ignored_count: length(ignored),
        ignored_reason_counts: reason_counts(ignored)
      }
    )
  end

  defp emit_ignored_dag_values([]), do: :ok

  defp emit_ignored_dag_values(ignored) do
    ignored
    |> Enum.group_by(& &1.reason)
    |> Enum.each(fn {reason, ignored} ->
      node_ids = Enum.map(ignored, & &1.node_id)

      :telemetry.execute(
        [:upkeep, :live, :dag_values, :ignored],
        %{count: length(ignored)},
        %{reason: reason, node_id: single_node_id(node_ids), node_ids: node_ids}
      )
    end)
  end

  defp reason_counts(ignored) do
    Map.new(Enum.group_by(ignored, & &1.reason), fn {reason, ignored} ->
      {reason, length(ignored)}
    end)
  end

  defp single_node_id([node_id]), do: node_id
  defp single_node_id(_node_ids), do: nil
end
