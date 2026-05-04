defmodule Upkeep.Coordinator.Graph do
  @moduledoc """
  Sharded graph-aware coordinator with `Group`-based interest tracking.

  Subscribers register interest in nodes (`register_source/4`,
  `register_derived/3`) declaring how their value is computed. The
  coordinator owns the routing index and the canonical node values; each
  shard:

    1. Looks up affected source nodes via an ETS index (no event_keys
       subset enumeration at notify time).
    2. Buffers, then on flush:
       - dedups events,
       - runs `load_fn/0` once per dirty source node,
       - recomputes pure derived nodes whose deps changed via the
         per-shard `Upkeep.DAG`,
       - dispatches values via `Group.dispatch/3`.

  ## Why Group

    * Per-pid GC: subscriber death triggers `:left` events; shards drop
      refcounted nodes when the last subscriber leaves. No hand-rolled
      `Process.monitor` bookkeeping.
    * Shape B lifecycle: shards `Group.register/4` themselves under
      `graph/shard/<idx>`. LVs `Group.monitor/2` that prefix and
      re-register their watches on shard restart.
    * Cross-node ready: `Group.dispatch/3` already does cross-node send
      batching when we go multi-node.

  ## Correctness contract

    * Subscribers treat values as authoritative state, not deltas.
    * Cross-shard ordering is not preserved (eventually consistent).
    * Bounded buffers block publishers; never silently drop events.
    * Derived nodes must colocate with their deps (raises otherwise).
  """

  use Supervisor

  alias Upkeep.Coordinator.Graph.Index
  alias Upkeep.Source

  @group Upkeep.Group
  @backpressure_threshold 5_000

  ## ETS table names
  @index_table :upkeep_graph_index
  @nodes_table :upkeep_graph_nodes

  ## Public API

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def group, do: @group

  @doc """
  Register a source node. Caller pid joins as subscriber via `Group.join/4`.

  The coordinator stores `{source, params}` and invokes `Upkeep.Source.load/2`
  during flush. Keeping source definitions as data makes shard state easier to
  inspect and avoids using anonymous closures as the production node contract.
  """
  def register_source(node_id, interest_keys, source, params)
      when is_list(interest_keys) and is_atom(source) and is_map(params) do
    shard = shard_of(node_id)

    :ok =
      GenServer.call(
        shard_name(shard),
        {:register_source, node_id, interest_keys, {:source, source, params}}
      )

    join_subscriber(node_id)
  end

  @doc """
  Register a source node and return the current shared value.

  If the same source identity is already loading, the caller joins that in-flight
  load and receives the shared result. Otherwise the shard starts a fresh load,
  stores its value for later notifications, reconciles its interest keys, and
  returns the loaded value.
  """
  def register_source_and_load(node_id, interest_keys, source, params)
      when is_list(interest_keys) and is_atom(source) and is_map(params) do
    shard = shard_of(node_id)

    {:ok, value, deps} =
      GenServer.call(
        shard_name(shard),
        {:register_source_and_load, node_id, interest_keys, {:source, source, params}},
        30_000
      )

    join_subscriber(node_id)
    {:ok, value, deps}
  end

  @doc """
  Register a test or benchmark loader function.

  Application sources should use `register_source/4`; this helper keeps
  low-level coordinator tests and benchmarks independent from Ecto sources.
  """
  def register_loader(node_id, interest_keys, load_fn)
      when is_list(interest_keys) and is_function(load_fn, 0) do
    shard = shard_of(node_id)

    :ok =
      GenServer.call(
        shard_name(shard),
        {:register_source, node_id, interest_keys, {:fun, load_fn}}
      )

    join_subscriber(node_id)
  end

  @doc """
  Register a derived node. Derived nodes colocate with their deps' shard;
  raises if deps span shards. Caller pid joins as subscriber.
  """
  def register_derived(node_id, dep_node_ids, compute_fn)
      when is_list(dep_node_ids) and is_function(compute_fn, 1) do
    target_shard = derived_shard!(node_id, dep_node_ids)

    :ok =
      GenServer.call(
        shard_name(target_shard),
        {:register_derived, node_id, dep_node_ids, compute_fn}
      )

    join_subscriber(node_id)
  end

  @doc """
  Return an initial derived value using a shared in-flight compute.

  Like `register_source_and_load/4`, concurrent callers for the same derived
  node join one in-flight initial compute. This does not subscribe callers to
  later derived dispatches; steady-state graph-derived subscriptions are handled
  by `register_derived/3`.
  """
  def register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata \\ %{})
      when is_list(dep_node_ids) and is_map(dep_values) and is_function(compute_fn, 1) and
             is_map(metadata) do
    target_shard = shard_of(node_id)

    {:ok, value} =
      GenServer.call(
        shard_name(target_shard),
        {:register_derived_and_compute, node_id, dep_node_ids, dep_values, compute_fn, metadata},
        30_000
      )

    {:ok, value}
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

  @doc """
  Caller pid leaves as a subscriber. The node itself is removed when the
  last subscriber leaves — the shard handles that reactively via Group's
  `:left` events.
  """
  def unregister(node_id) do
    case Group.leave(@group, source_key(node_id)) do
      :ok -> :ok
      {:error, :not_in_group} -> :ok
    end
  end

  @doc "Return the set of pids currently subscribed to `node_id`."
  def subscribers(node_id) do
    @group
    |> Group.members(source_key(node_id))
    |> Enum.map(fn {pid, _meta} -> pid end)
    |> MapSet.new()
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
      |> Index.lookup_nodes()
      |> Enum.group_by(&shard_of_node/1)

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

  @doc false
  def reset do
    shards = :persistent_term.get({__MODULE__, :shards})

    for idx <- 0..(shards - 1), do: GenServer.call(shard_name(idx), :reset, 60_000)

    :ets.delete_all_objects(@index_table)
    :ets.delete_all_objects(@nodes_table)

    :ok
  end

  def shard_name(idx), do: :"#{__MODULE__}.Shard.#{idx}"
  def task_sup, do: :"#{__MODULE__}.TaskSup"

  def shard_count, do: :persistent_term.get({__MODULE__, :shards})

  @doc false
  def shared_partition_info(node_ids) do
    dep_partitions = Enum.map(node_ids, &{&1, node_partition(&1)})

    case dep_partitions |> Enum.map(&elem(&1, 1)) |> Enum.uniq() do
      [partition] ->
        {:ok, partition, dep_partitions}

      [] ->
        {:error, :empty_deps, dep_partitions}

      _partitions ->
        {:error, :cross_partition_dep, dep_partitions}
    end
  end

  @doc false
  def shared_partition(node_ids) do
    case shared_partition_info(node_ids) do
      {:ok, partition, _dep_partitions} -> {:ok, partition}
      {:error, reason, _dep_partitions} -> {:error, reason}
    end
  end

  @doc false
  def node_partition({source, params}) when is_atom(source) and is_map(params) do
    Source.sharing_partition(source, params)
  end

  def node_partition({:derived, _view, _assign_name, dep_node_ids, _fun}) do
    case shared_partition(dep_node_ids) do
      {:ok, partition} -> partition
      {:error, reason} -> {:derived, reason, dep_node_ids}
    end
  end

  def node_partition(node_id), do: {:node, node_id}

  @doc "Group key under which a node's subscribers join."
  def source_key(node_id) do
    "graph/source/" <>
      (node_id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false))
  end

  def decode_source_key("graph/source/" <> encoded) do
    encoded
    |> Base.url_decode64!(padding: false)
    |> :erlang.binary_to_term()
  end

  @doc "Group key under which a shard registers itself for lifecycle monitoring."
  def shard_key(idx), do: "graph/shard/#{idx}"

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
    :erlang.phash2(node_partition(node_id), shards)
  end

  # Sources hash to their shard; derived nodes have an explicit override
  # stored in nodes_table so they can colocate with their deps.
  defp shard_of_node(node_id) do
    case Index.lookup(node_id) do
      {:ok, {_kind, shard_idx, _keys}} -> shard_idx
      _ -> shard_of(node_id)
    end
  end

  defp join_subscriber(node_id) do
    case Group.join(@group, source_key(node_id), %{kind: :lv}) do
      :ok -> :ok
      :already_joined -> :ok
    end
  end

  ## Shared accessors used by Shard

  @doc false
  def index_table, do: @index_table
  @doc false
  def nodes_table, do: @nodes_table
end
