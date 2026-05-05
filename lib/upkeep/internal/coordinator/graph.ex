defmodule Upkeep.Internal.Coordinator.Graph do
  @moduledoc false

  use Supervisor

  alias Upkeep.Internal.Coordinator.ReadNodes
  alias Upkeep.Internal.Coordinator.Graph.Notifier
  alias Upkeep.Internal.Coordinator.Topology

  @group Upkeep.Group
  @notification_key "graph/notifications"

  ## Public API

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def group, do: @group
  def notification_key, do: @notification_key

  @doc """
  Register a source node. Caller pid joins as subscriber via `Group.join/4`.

  The coordinator stores `{source, params}` and invokes `Upkeep.Source.load/2`
  during flush. Keeping source definitions as data makes shard state easier to
  inspect and avoids using anonymous closures as the production node contract.
  """
  def register_source(node_id, interest_keys, source, params)
      when is_list(interest_keys) and is_atom(source) and is_map(params) do
    shard = Topology.shard_of(node_id)

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
    shard = Topology.shard_of(node_id)

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
    shard = Topology.shard_of(node_id)

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
    target_shard = Topology.shard_of(node_id)

    {:ok, value} =
      GenServer.call(
        shard_name(target_shard),
        {:register_derived_and_compute, node_id, dep_node_ids, dep_values, compute_fn, metadata},
        30_000
      )

    {:ok, value}
  end

  defp derived_shard!(node_id, dep_node_ids) do
    groups = Topology.dependency_shard_groups(dep_node_ids)

    case groups do
      [%{shard: shard}] ->
        shard

      [] ->
        raise ArgumentError,
              "register_derived/3 for #{inspect(node_id)} requires at least one dep"

      [_ | _] = multi ->
        shards = Enum.map(multi, & &1.shard)

        raise ArgumentError,
              "register_derived/3 for #{inspect(node_id)} has deps split across shards " <>
                "#{inspect(shards)}. #{cross_shard_message(multi)} " <>
                "cross-shard recompute is not implemented yet."
    end
  end

  defp cross_shard_message(groups) do
    largest_count = groups |> Enum.map(& &1.count) |> Enum.max()
    largest = Enum.filter(groups, &(&1.count == largest_count))

    "The largest colocated dependency group is " <>
      Enum.map_join(largest, "; ", fn group ->
        "shard #{group.shard} with #{group.count} dep(s): #{inspect(group.node_ids)}"
      end) <>
      ". Reshape the derived node around the largest group, or keep this compute local. "
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
    ReadNodes.invalidate(event)
    Notifier.notify(event)
    Group.dispatch(@group, @notification_key, {:upkeep_graph_notify, node(), event})
  end

  @doc "Synchronously drain all shards."
  def drain do
    Notifier.drain()

    for idx <- 0..(Topology.shard_count() - 1),
        do: GenServer.call(shard_name(idx), :drain, 60_000)

    :ok
  end

  @doc false
  def reset do
    for idx <- 0..(Topology.shard_count() - 1),
        do: GenServer.call(shard_name(idx), :reset, 60_000)

    Topology.reset()
    ReadNodes.clear()

    :ok
  end

  def shard_name(idx), do: :"#{__MODULE__}.Shard.#{idx}"
  def task_sup, do: :"#{__MODULE__}.TaskSup"

  defdelegate shard_count, to: Topology

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

  @doc "Group key under which a shard joins for lifecycle monitoring."
  def shard_key(idx), do: "graph/shard/#{idx}"

  ## Supervisor

  @impl true
  def init(opts) do
    shards = Keyword.get(opts, :shards, System.schedulers_online())
    Topology.put_shard_count(shards)
    Topology.init_tables()

    Enum.each(ReadNodes.table_specs(), fn {name, opts} -> ensure_table(name, opts) end)

    children = [
      {Upkeep.Internal.SingleFlight.Registry,
       name: Upkeep.Internal.Coordinator.ReadNodes.Coalescer,
       telemetry_prefix: [:upkeep, :read_nodes]},
      Upkeep.Internal.Coordinator.ReadNodes.Watcher,
      Upkeep.Internal.Coordinator.Graph.Notifier,
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

  defp join_subscriber(node_id) do
    case Group.join(@group, source_key(node_id), %{kind: :lv}) do
      :ok -> :ok
      :already_joined -> :ok
    end
  end
end
