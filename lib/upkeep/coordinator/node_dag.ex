defmodule Upkeep.Coordinator.NodeDAG do
  @moduledoc """
  Sharded centralized DAG coordinator.

  Subscribers register interest in nodes (`register_source/3`,
  `register_derived/3`) declaring how their value is computed. The
  coordinator owns the routing index and the canonical node values, and
  on `notify/1`:

    1. Looks up affected source nodes via an ETS index (no event_keys
       subset enumeration at notify time).
    2. Partitions by shard and dispatches one cast (or call under
       backpressure) per shard with the affected node_ids.
    3. Each shard buffers, then on flush:
       - dedups events,
       - runs `load_fn/0` once per dirty source node,
       - recomputes pure derived nodes whose deps changed,
       - sends `{:dag_value, node_id, value}` to interested pids,
       - dispatches in parallel under `Task.Supervisor`.

  ## Optimizations applied

    * Sharding by node_id (parallelism).
    * Cast fast path with call-on-pressure backpressure (bounded mailbox,
      fast publishers under normal load, blocked publishers under burst).
    * Flush buffer with coalescing window (collapse repeated events
      affecting the same node within a tick).
    * Parallel dispatch via `Task.Supervisor` so a slow load_fn doesn't
      block the shard.
    * ETS-backed shared index for O(matching keys) notify routing.

  ## NodeDAG-only optimizations

    * Derived-node recompute: when a source changes, dependent derived
      nodes recompute *once* across all subscribers, not once per LV.
    * Indexed routing: notify cost is O(matching index entries), not
      O(event field subsets).

  ## Correctness contract

    * Subscribers treat values as authoritative state, not deltas.
    * Coalescing collapses semantically-distinct events only when they
      are `==`; see `Upkeep.ChangeEqualityTest`.
    * Cross-shard ordering is not preserved (eventually consistent).
    * Bounded buffers block publishers; never silently drop events.
  """

  use Supervisor

  alias Upkeep.Source

  @backpressure_threshold 5_000

  ## ETS table names
  @index_table :upkeep_node_dag_index
  @nodes_table :upkeep_node_dag_nodes

  ## Public API

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Register a source node. `interest_keys` are notification keys this node
  reacts to; `load_fn` is a 0-arity function returning the current value.
  Idempotent: re-registering with the same `node_id` adds the caller pid
  to the interest set without redefining the node.
  """
  def register_source(node_id, interest_keys, load_fn, pid \\ nil)
      when is_list(interest_keys) and is_function(load_fn, 0) do
    pid = pid || self()
    shard = shard_of(node_id)
    GenServer.call(shard_name(shard), {:register_source, pid, node_id, interest_keys, load_fn})
  end

  @doc """
  Register a derived node. `dep_node_ids` must already be registered.
  `compute_fn` takes a map of `dep_node_id => value` and returns the
  derived value.

  Derived nodes are placed on the same shard as their dependencies so
  recompute can run locally. If `dep_node_ids` span multiple shards this
  raises — a deliberate guardrail until cross-shard recompute exists.
  """
  def register_derived(node_id, dep_node_ids, compute_fn, pid \\ nil)
      when is_list(dep_node_ids) and is_function(compute_fn, 1) do
    pid = pid || self()
    target_shard = derived_shard!(node_id, dep_node_ids)

    GenServer.call(
      shard_name(target_shard),
      {:register_derived, pid, node_id, dep_node_ids, compute_fn}
    )
  end

  defp derived_shard!(node_id, dep_node_ids) do
    case dep_node_ids |> Enum.map(&shard_of_node/1) |> Enum.uniq() do
      [shard] ->
        shard

      [] ->
        raise ArgumentError,
              "register_derived/3 for #{inspect(node_id)} requires at least one dep"

      multiple ->
        raise ArgumentError,
              "register_derived/3 for #{inspect(node_id)} has deps split across shards " <>
                "#{inspect(multiple)}. Reshape node_ids so all deps share a shard, or " <>
                "wait for cross-shard recompute support."
    end
  end

  def unregister(node_id, pid \\ nil) do
    pid = pid || self()
    GenServer.call(shard_name(shard_of_node(node_id)), {:unregister, pid, node_id})
  end

  @doc "Return the set of pids currently subscribed to `node_id`."
  def subscribers(node_id) do
    GenServer.call(shard_name(shard_of_node(node_id)), {:subscribers, node_id})
  end

  @doc "Return true if `pid` is currently subscribed to `node_id`."
  def subscribed?(node_id, pid \\ nil) do
    pid = pid || self()
    MapSet.member?(subscribers(node_id), pid)
  end

  def notify(event) when is_struct(event) do
    affected_by_shard =
      event
      |> Source.event_keys()
      |> Enum.flat_map(&:ets.lookup(@index_table, &1))
      |> Enum.map(fn {_key, node_id} -> node_id end)
      |> Enum.uniq()
      |> Enum.group_by(&shard_of/1)

    Enum.each(affected_by_shard, fn {shard, node_ids} ->
      pid = Process.whereis(shard_name(shard))

      case Process.info(pid, :message_queue_len) do
        {:message_queue_len, len} when len > @backpressure_threshold ->
          GenServer.call(pid, {:notify, event, node_ids}, 30_000)

        _ ->
          GenServer.cast(pid, {:notify, event, node_ids})
      end
    end)
  end

  @doc "Synchronously drain all shards."
  def drain do
    shards = :persistent_term.get({__MODULE__, :shards})
    for idx <- 0..(shards - 1), do: GenServer.call(shard_name(idx), :drain, 60_000)
    :ok
  end

  def shard_name(idx), do: :"#{__MODULE__}.Shard.#{idx}"
  def task_sup, do: :"#{__MODULE__}.TaskSup"

  ## Supervisor

  @impl true
  def init(opts) do
    shards = Keyword.get(opts, :shards, System.schedulers_online())
    :persistent_term.put({__MODULE__, :shards}, shards)

    ensure_table(@index_table, [
      :bag,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    ensure_table(@nodes_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    children = [
      {Task.Supervisor, name: task_sup()}
      | for idx <- 0..(shards - 1) do
          Supervisor.child_spec(
            {__MODULE__.Shard, name: shard_name(idx), idx: idx},
            id: {__MODULE__.Shard, idx}
          )
        end
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp ensure_table(name, opts) do
    case :ets.info(name) do
      :undefined -> :ets.new(name, opts)
      _ -> :ok
    end
  end

  defp shard_of(node_id) do
    shards = :persistent_term.get({__MODULE__, :shards})
    :erlang.phash2(node_id, shards)
  end

  # Sources hash to their shard; derived nodes have an explicit override
  # stored in nodes_table so they can colocate with their deps.
  defp shard_of_node(node_id) do
    case :ets.lookup(@nodes_table, node_id) do
      [{^node_id, _kind, shard_idx}] -> shard_idx
      _ -> shard_of(node_id)
    end
  end

  ## Shared accessors used by Shard

  @doc false
  def index_table, do: @index_table
  @doc false
  def nodes_table, do: @nodes_table
end
