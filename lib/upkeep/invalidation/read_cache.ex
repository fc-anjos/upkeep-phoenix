defmodule Upkeep.Invalidation.ReadCache do
  @moduledoc false

  alias Upkeep.InvalidationSurface.Index.ETS, as: SurfaceIndex
  alias Upkeep.SingleFlight.Registry

  @values :upkeep_read_node_values
  @index :upkeep_read_node_index
  @refs :upkeep_read_node_refs
  def values_table, do: @values
  def index_table, do: @index
  def refs_table, do: @refs

  def table_specs do
    [
      {@values, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@index, [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@refs, [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true]}
    ]
  end

  def fetch_or_load(node_id, deps, load, holder \\ nil) when is_function(load, 0) do
    value =
      case :ets.lookup(@values, node_id) do
        [{^node_id, value}] ->
          value

        [] ->
          Registry.coalesce(coalescer_name(), node_id, fn ->
            # Re-check ETS inside the single-flight critical section:
            # a concurrent caller may have settled while we waited to
            # be the loader.
            case :ets.lookup(@values, node_id) do
              [{^node_id, value}] ->
                value

              [] ->
                value = load.()
                :ets.insert(@values, {node_id, value})
                surface = Upkeep.Source.dependency_surface(List.wrap(deps))
                SurfaceIndex.insert(@index, node_id, surface, surface)

                value
            end
          end)
      end

    if holder, do: :ets.insert(@refs, {holder, node_id})
    value
  end

  def release(holder) do
    held = :ets.lookup(@refs, holder)
    :ets.delete(@refs, holder)

    held
    |> Enum.map(fn {^holder, node_id} -> node_id end)
    |> Enum.uniq()
    |> Enum.reduce(0, fn node_id, acc ->
      if any_holder?(node_id) do
        acc
      else
        evict_unheld(node_id)
        acc + 1
      end
    end)
  end

  defp any_holder?(node_id) do
    case :ets.select(@refs, [{{:"$1", node_id}, [], [true]}], 1) do
      :"$end_of_table" -> false
      _ -> true
    end
  end

  defp evict_unheld(node_id) do
    :ets.delete(@values, node_id)
    SurfaceIndex.delete(@index, node_id)
    :ok
  end

  def invalidate(event) when is_struct(event) do
    {candidate_keys, candidates} = SurfaceIndex.candidates(@index, event)

    evicted_count =
      Enum.reduce(candidates, 0, fn {node_id, surface}, acc ->
        if Upkeep.InvalidationSurface.matches?(surface, event) do
          evict(node_id)
          acc + 1
        else
          acc
        end
      end)

    emit_invalidation(event, candidate_keys, candidates, evicted_count)

    evicted_count
  end

  def invalidate(_event), do: 0

  def clear do
    :ets.delete_all_objects(@values)
    :ets.delete_all_objects(@index)
    :ets.delete_all_objects(@refs)
    :ok
  end

  def count, do: :ets.info(@values, :size)
  def coalescer_name, do: Upkeep.Invalidation.ReadCache.Coalescer

  defp evict(node_id) do
    :ets.delete(@values, node_id)
    SurfaceIndex.delete(@index, node_id)

    # Refs from holders to this read-node are stale once the value is
    # gone; let the next fetch_or_load re-establish them.
    :ets.match_delete(@refs, {:_, node_id})
  end

  defp emit_invalidation(event, candidate_keys, candidates, evicted_count) do
    :telemetry.execute(
      [:upkeep, :read_nodes, :invalidation],
      %{
        count: 1,
        candidate_key_count: length(candidate_keys),
        candidate_count: length(candidates),
        evicted_count: evicted_count
      },
      Upkeep.InvalidationSurface.event_metadata(event)
    )
  end
end
