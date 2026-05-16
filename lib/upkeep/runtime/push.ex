defmodule Upkeep.Runtime.Push do
  @moduledoc false

  alias Upkeep.Runtime.Patch
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Watches

  def apply_dag_values(socket, pairs) when is_list(pairs) do
    started_at = System.monotonic_time()

    patch =
      pairs
      |> Enum.reduce(Patch.new(socket), fn {node_id, value}, patch ->
        put_pushed_value(patch, node_id, value)
      end)
      |> Patch.recompute(&Watches.remove_watch/2)

    emit_apply_dag_values(
      pairs,
      Patch.changed_nodes(patch),
      Patch.effects(patch),
      Patch.recompute_effects(patch),
      Patch.ignored(patch),
      started_at
    )

    emit_ignored_dag_values(Patch.ignored(patch))

    Patch.result(patch)
  end

  def apply_dag_value(socket, source_id, value) do
    patch =
      socket
      |> Patch.new()
      |> put_pushed_value(source_id, value)
      |> Patch.recompute(&Watches.remove_watch/2)

    emit_ignored_dag_values(Patch.ignored(patch))

    Patch.result(patch)
  end

  defp put_pushed_value(%Patch{} = patch, source_id, value) do
    case Map.fetch(State.watches(Patch.socket(patch)), source_id) do
      {:ok, watch} ->
        Patch.put_watch_value(patch, watch, value)

      :error ->
        Patch.ignore(patch, ignored_reason(source_id), source_id)
    end
  end

  defp ignored_reason({_source, params}) when is_map(params), do: :unwatched_source

  defp ignored_reason({:scoped, _component, {_source, params}}) when is_map(params),
    do: :unwatched_source

  defp ignored_reason(_node_id), do: :stale_push

  defp emit_apply_dag_values(
         pairs,
         changed_nodes,
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
