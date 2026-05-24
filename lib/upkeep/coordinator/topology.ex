defmodule Upkeep.Coordinator.Topology do
  @moduledoc false

  alias Upkeep.InvalidationSurface
  alias Upkeep.InvalidationSurface.Index.ETS, as: SurfaceIndex
  alias Upkeep.Source.Identity, as: SourceIdentity

  @nodes_table :upkeep_topology_nodes
  @index_table :upkeep_topology_index
  @generations_table :upkeep_topology_generations

  @doc """
  Table specs (`{name, ets_opts}`) for the topology routing index.

  Used by the dedicated table owner so the tables outlive supervisor restarts
  (see `Upkeep.ETS.TableOwner`).
  """
  def table_specs do
    [
      {@nodes_table,
       [:set, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@index_table,
       [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@generations_table,
       [:set, :public, :named_table, read_concurrency: true, write_concurrency: true]}
    ]
  end

  def init_tables do
    Enum.each(table_specs(), fn {name, opts} -> :ok = ensure_named_table!(name, opts) end)
    :ok
  end

  def reset do
    :ets.delete_all_objects(@nodes_table)
    :ets.delete_all_objects(@index_table)
    :ets.delete_all_objects(@generations_table)
    :ok
  end

  def register_source(node_id, %InvalidationSurface{} = surface) do
    :ets.insert(@nodes_table, {node_id, %{kind: :source, surface: surface, deps: []}})

    ensure_generation(node_id)
    SurfaceIndex.insert(@index_table, node_id, surface)
    :ok
  end

  def reconcile_source(node_id, %InvalidationSurface{} = new_surface) do
    SurfaceIndex.replace(@index_table, node_id, new_surface)
    :ets.insert(@nodes_table, {node_id, %{kind: :source, surface: new_surface, deps: []}})
    :ok
  end

  def unregister(node_id) do
    case lookup(node_id) do
      {:ok, %{kind: :source}} ->
        SurfaceIndex.delete(@index_table, node_id)
        :ets.delete(@nodes_table, node_id)
        :ets.delete(@generations_table, node_id)
        :ok

      {:ok, _node} ->
        :ets.delete(@nodes_table, node_id)
        :ok

      :error ->
        :ok
    end
  end

  def lookup(node_id) do
    case :ets.lookup(@nodes_table, node_id) do
      [{^node_id, %{kind: :source, surface: surface} = node}] ->
        {:ok, Map.put(node, :surface_keys, InvalidationSurface.index_keys(surface))}

      [{^node_id, node}] ->
        {:ok, node}

      _ ->
        :error
    end
  end

  def registered?(node_id) do
    match?({:ok, _}, lookup(node_id))
  end

  def generation(node_id) do
    :ets.update_counter(@generations_table, node_id, {2, 0}, {node_id, 0})
  end

  def affected_source_node_ids(event) when is_struct(event) do
    {candidate_keys, candidates} = SurfaceIndex.candidates(@index_table, event)
    candidate_node_ids = Enum.map(candidates, fn {node_id, _payload} -> node_id end)

    matched_node_ids = Enum.filter(candidate_node_ids, &source_node_matches?(&1, event))
    bump_generations(matched_node_ids)

    emit_invalidation(event, candidate_keys, candidate_node_ids, matched_node_ids)

    matched_node_ids
  end

  def node_partition({source, params}) when is_atom(source) and is_map(params) do
    SourceIdentity.sharing_partition(source, params)
  end

  def node_partition({:identity, {source, params}, identity_envelope})
      when is_atom(source) and is_map(params) do
    {:identity, SourceIdentity.sharing_partition(source, params), identity_envelope}
  end

  def node_partition(node_id), do: {:node, node_id}

  defp ensure_named_table!(name, opts) do
    case :ets.whereis(name) do
      :undefined ->
        ^name = :ets.new(name, opts)
        :ok

      _ ->
        :ok
    end
  end

  defp ensure_generation(node_id) do
    :ets.insert_new(@generations_table, {node_id, 0})
    :ok
  end

  defp bump_generations(node_ids) do
    Enum.each(node_ids, fn node_id ->
      :ets.update_counter(@generations_table, node_id, {2, 1}, {node_id, 0})
    end)
  end

  defp source_node_matches?(node_id, event) do
    case lookup(node_id) do
      {:ok, %{kind: :source, surface: surface}} -> InvalidationSurface.matches?(surface, event)
      _ -> false
    end
  end

  defp emit_invalidation(event, candidate_keys, candidate_node_ids, matched_node_ids) do
    :telemetry.execute(
      [:upkeep, :graph, :invalidation],
      %{
        count: 1,
        candidate_key_count: length(candidate_keys),
        candidate_count: length(candidate_node_ids),
        matched_count: length(matched_node_ids)
      },
      InvalidationSurface.event_metadata(event)
    )
  end
end
