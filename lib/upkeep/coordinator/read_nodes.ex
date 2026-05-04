defmodule Upkeep.Coordinator.ReadNodes do
  @moduledoc """
  Library-level read-node graph.

  A *read-node* is a first-class node keyed by `{repo, fingerprint(query)}`
  whose value is the result of running that query against the database.
  Source `load/1` callbacks no longer hit the database directly — every
  `Upkeep.read/1` looks up (or installs) a read-node, so two callbacks
  that need the same query share one fetch and one cached value.

  ## Lookup vs. invalidation

  Read-nodes are indexed under a *coarse* key, `{action, schema}`, instead
  of registering against the same explosive interest-key set the source
  graph uses. A typical write (e.g. `inserted` of a struct with N fields)
  generates `2^N` interest keys for source-node matching; replicating that
  work synchronously in the mutator's process turned out to dominate
  end-to-end submit latency. Read-nodes only need a per-schema invalidation
  surface, since their precise dependencies are recomputed via
  `Upkeep.Ecto.QueryDeps.matches_change?/2` against the candidate set —
  cheap and exact.

  Cached values live in a single public ETS table so any process can hit
  them without cross-shard messaging. The supporting tables are owned by
  the `Upkeep.Coordinator.Graph` supervisor and share its lifecycle.
  """

  alias Upkeep.Coordinator.ReadNodes.Coalescer
  alias Upkeep.Ecto.QueryDeps

  @values :upkeep_read_node_values
  @index :upkeep_read_node_index
  @refs :upkeep_read_node_refs

  @doc false
  def values_table, do: @values
  @doc false
  def index_table, do: @index
  @doc false
  def refs_table, do: @refs

  @doc false
  def table_specs do
    [
      {@values, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@index, [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true]},
      {@refs, [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true]}
    ]
  end

  @doc """
  Look up the cached value for `query` against `repo`, or run `repo.all/1`
  to populate it. Returns the result list.
  """
  def fetch_or_load(repo, %Ecto.Query{} = query, holder \\ nil) do
    fp = fingerprint(repo, query)
    node_id = {:read, repo, fp}

    value =
      case :ets.lookup(@values, node_id) do
        [{^node_id, value}] ->
          value

        [] ->
          Coalescer.coalesce(node_id, fn ->
            # Re-check ETS inside the single-flight critical section:
            # a concurrent caller may have settled while we waited to
            # be the loader.
            case :ets.lookup(@values, node_id) do
              [{^node_id, value}] ->
                value

              [] ->
                deps = QueryDeps.from_query(query)
                value = repo.all(query)
                :ets.insert(@values, {node_id, value})

                Enum.each(coarse_keys(deps), fn key ->
                  :ets.insert(@index, {key, {node_id, deps}})
                end)

                value
            end
          end)
      end

    if holder, do: :ets.insert(@refs, {holder, node_id})
    value
  end

  @doc """
  Release every read-node held by `holder` (a source node id). Read-nodes
  with no remaining holders are evicted along with their index entries.

  Returns the count of evicted read-nodes.
  """
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
    :ets.match_delete(@index, {:_, {node_id, :_}})
    :ok
  end

  @doc """
  Evict every read-node whose query matches `event`.

  Returns the count of evicted read-nodes.
  """
  def invalidate(%{action: _, schema: _} = event) do
    event
    |> candidate_keys()
    |> Enum.flat_map(&:ets.lookup(@index, &1))
    |> Enum.uniq_by(fn {_key, {node_id, _deps}} -> node_id end)
    |> Enum.reduce(0, fn {_key, {node_id, deps}}, acc ->
      if QueryDeps.matches_change?(deps, event) do
        evict(node_id, deps)
        acc + 1
      else
        acc
      end
    end)
  end

  def invalidate(_event), do: 0

  @doc """
  Drop every cached read-node and reset the index.
  """
  def clear do
    :ets.delete_all_objects(@values)
    :ets.delete_all_objects(@index)
    :ets.delete_all_objects(@refs)
    :ok
  end

  @doc false
  def count, do: :ets.info(@values, :size)

  defp evict(node_id, deps) do
    :ets.delete(@values, node_id)

    Enum.each(coarse_keys(deps), fn key ->
      :ets.match_delete(@index, {key, {node_id, :_}})
    end)

    # Refs from holders to this read-node are stale once the value is
    # gone; let the next fetch_or_load re-establish them.
    :ets.match_delete(@refs, {:_, node_id})
  end

  defp coarse_keys(%QueryDeps{schemas: schemas}) do
    actions = [:inserted, :updated, :deleted]

    for schema <- schemas, action <- actions do
      {action, schema}
    end
  end

  defp candidate_keys(%{action: action, schema: schema}) when not is_nil(schema) do
    [{action, schema}]
  end

  defp candidate_keys(_event), do: []

  defp fingerprint(repo, query) do
    {sql, params} = Ecto.Adapters.SQL.to_sql(:all, repo, query)
    :erlang.phash2({sql, params})
  end
end
