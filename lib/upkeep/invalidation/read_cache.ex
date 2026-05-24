defmodule Upkeep.Invalidation.ReadCache do
  @moduledoc false

  alias Upkeep.InvalidationSurface.Index.ETS, as: SurfaceIndex
  alias Upkeep.SingleFlight.Registry
  alias Upkeep.Source.Dependencies

  @values :upkeep_read_node_values
  @index :upkeep_read_node_index
  @refs :upkeep_read_node_refs
  @generations :upkeep_read_node_generations
  def values_table, do: @values
  def index_table, do: @index
  def refs_table, do: @refs
  def generations_table, do: @generations

  def table_specs do
    [
      {@values, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@index, [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@refs, [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@generations,
       [:set, :public, :named_table, read_concurrency: true, write_concurrency: true]}
    ]
  end

  def fetch_or_load(node_id, deps, load, holder \\ nil) when is_function(load, 0) do
    value =
      case :ets.lookup(@values, node_id) do
        [{^node_id, :loaded, value, _gen}] ->
          value

        _ ->
          Registry.coalesce(coalescer_name(), node_id, fn -> load_cached(node_id, deps, load) end)
      end

    if holder, do: :ets.insert(@refs, {holder, node_id})
    value
  end

  defp load_cached(node_id, deps, load) do
    case :ets.lookup(@values, node_id) do
      [{^node_id, :loaded, value, _gen}] -> value
      _ -> load_fresh(node_id, deps, load)
    end
  end

  defp load_fresh(node_id, deps, load) do
    surface = Dependencies.surface(List.wrap(deps))
    gen_before = read_gen(node_id)

    SurfaceIndex.insert(@index, node_id, surface, surface)
    :ets.delete(@values, node_id)
    true = :ets.insert_new(@values, {node_id, :loading, nil, gen_before})

    if read_gen(node_id) != gen_before do
      :ets.delete(@values, node_id)
      SurfaceIndex.delete(@index, node_id)
      load.()
    else
      commit_load(node_id, load.(), gen_before)
    end
  end

  # Atomic compare-and-swap: the row is promoted only if its `gen` field is
  # still `gen_before`. `evict/1` bumps the generation in @generations AND
  # deletes the @values row, so either condition breaks the match. Tuple
  # values nested in the body must be wrapped in `{:const, term}` or ETS
  # rejects the match spec.
  defp commit_load(node_id, value, gen_before) do
    match_spec = [
      {{node_id, :loading, :_, gen_before}, [],
       [{{{:const, node_id}, :loaded, {:const, value}, gen_before}}]}
    ]

    case :ets.select_replace(@values, match_spec) do
      1 ->
        value

      0 ->
        SurfaceIndex.delete(@index, node_id)
        :ets.delete(@values, node_id)
        value
    end
  end

  defp read_gen(node_id) do
    :ets.update_counter(@generations, node_id, {2, 0}, {node_id, 0})
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
    :ets.delete(@generations, node_id)
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
    :ets.delete_all_objects(@generations)
    :ok
  end

  def count, do: :ets.info(@values, :size)
  def coalescer_name, do: Upkeep.Invalidation.ReadCache.Coalescer

  # Bump the generation BEFORE deleting the @values row so an in-flight
  # loader's `commit_load` CAS finds either a missing row or a row with a
  # stale gen and skips caching. We delete the @values row before the
  # @generations row so the CAS guard (which keys off the @values row) still
  # holds even if a concurrent in-flight loader re-creates the @generations
  # row at 0 via `read_gen`. Deleting the @generations row keeps that table
  # from growing monotonically as nodes are evicted over time.
  defp evict(node_id) do
    :ets.update_counter(@generations, node_id, {2, 1}, {node_id, 0})
    :ets.delete(@values, node_id)
    SurfaceIndex.delete(@index, node_id)
    :ets.match_delete(@refs, {:_, node_id})
    :ets.delete(@generations, node_id)
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
