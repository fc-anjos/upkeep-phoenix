defmodule Upkeep.Coordinator.Graph do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Notifier
  alias Upkeep.Coordinator.Shards
  alias Upkeep.Coordinator.Subscriptions
  alias Upkeep.Coordinator.Topology
  alias Upkeep.Source.ReadCache

  ## Public API

  def start_link(opts \\ []) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    Upkeep.Coordinator.Supervisor.start_link(opts)
  end

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  defdelegate group, to: Subscriptions
  defdelegate notification_key, to: Subscriptions

  @doc """
  Register a source node. Caller pid joins as subscriber via `Group.join/4`.

  The coordinator stores `{source, params}` and invokes `Upkeep.Source.Loader.load/2`
  during flush. Keeping source definitions as data makes shard state easier to
  inspect and avoids using anonymous closures as the production node contract.
  """
  def register_source(node_id, interest_keys, source, params)
      when is_list(interest_keys) and is_atom(source) and is_map(params) do
    :ok = Shards.register_source(node_id, interest_keys, source, params)
    Subscriptions.subscribe(node_id)
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
    {:ok, value, deps} = Shards.register_source_and_load(node_id, interest_keys, source, params)
    Subscriptions.subscribe(node_id)
    {:ok, value, deps}
  end

  @doc """
  Register a test or benchmark loader function.

  Application sources should use `register_source/4`; this helper keeps
  low-level coordinator tests and benchmarks independent from Ecto sources.
  """
  def register_loader(node_id, interest_keys, load_fn)
      when is_list(interest_keys) and is_function(load_fn, 0) do
    :ok = Shards.register_loader(node_id, interest_keys, load_fn)
    Subscriptions.subscribe(node_id)
  end

  @doc """
  Register a derived node. Derived nodes colocate with their deps' shard;
  raises if deps span shards. Caller pid joins as subscriber.
  """
  def register_derived(node_id, dep_node_ids, compute_fn)
      when is_list(dep_node_ids) and is_function(compute_fn, 1) do
    :ok = Shards.register_derived(node_id, dep_node_ids, compute_fn)
    Subscriptions.subscribe(node_id)
  end

  @doc """
  Return an initial derived value using a shared in-flight compute.

  Like `register_source_and_load/4`, concurrent callers for the same derived
  node join one in-flight initial compute. This does not subscribe callers to
  later derived dispatches; steady-state graph-derived subscriptions are handled
  by `register_derived/3`.
  """
  def register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata \\ %{}) do
    Shards.register_derived_and_compute(node_id, dep_node_ids, dep_values, compute_fn, metadata)
  end

  @doc """
  Caller pid leaves as a subscriber. The node itself is removed when the
  last subscriber leaves — the shard handles that reactively via Group's
  `:left` events.
  """
  defdelegate unregister(node_id), to: Subscriptions, as: :unsubscribe

  @doc "Return the set of pids currently subscribed to `node_id`."
  defdelegate subscribers(node_id), to: Subscriptions

  @doc "Return true if `pid` is currently subscribed to `node_id`."
  defdelegate subscribed?(node_id, pid \\ nil), to: Subscriptions

  def notify(event) when is_struct(event) do
    ReadCache.invalidate(event)
    Notifier.notify(event)
    Subscriptions.dispatch_notification(event)
  end

  @doc "Synchronously drain all shards."
  def drain do
    Notifier.drain()
    Shards.drain_all()
  end

  @doc false
  def reset do
    Shards.reset_all()
    Topology.reset()
    ReadCache.clear()

    :ok
  end

  defdelegate shared_partition_info(node_ids), to: Topology

  defdelegate shard_name(idx), to: Shards, as: :name
  defdelegate task_sup, to: Shards
  defdelegate shard_count, to: Shards

  @doc "Group key under which a node's subscribers join."
  defdelegate source_key(node_id), to: Subscriptions

  defdelegate decode_source_key(key), to: Subscriptions

  @doc "Group key under which a shard joins for lifecycle monitoring."
  defdelegate shard_key(idx), to: Subscriptions
end
