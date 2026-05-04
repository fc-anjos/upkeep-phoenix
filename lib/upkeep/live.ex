defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.
  """

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Live.{
    Assigns,
    Components,
    DAGOperations,
    Ids,
    SharedDerived,
    Snapshot,
    SourceLoads,
    State,
    Subscriptions,
    Telemetry
  }

  defmacro __using__(_opts) do
    quote do
      import Upkeep.Live,
        only: [
          watch: 4,
          watch: 5,
          component: 4,
          remove_component: 2,
          derive: 4,
          unwatch: 2,
          unwatch: 3,
          refresh: 4
        ]

      @impl true
      def handle_info({:dag_values, pairs}, socket) do
        {:noreply, Upkeep.Live.apply_dag_values(socket, pairs)}
      end

      defoverridable handle_info: 2
    end
  end

  def watch(socket, assign_name, source, params, opts \\ []) when is_atom(assign_name) do
    params = normalize_params(params)
    component = Keyword.get(opts, :under)
    source_id = Ids.scoped_source_id(source, params, component)

    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        Telemetry.emit(
          [:source, :watch],
          %{count: 1},
          Telemetry.watch_metadata(watch, assign_name, :alias)
        )

        socket
        |> State.put_watch_assign(source_id, assign_name)
        |> State.put_assign_node(assign_name, Ids.source_node_id(source_id))
        |> assign(assign_name, Map.fetch!(socket.assigns, primary_assign_name(watch)))

      :error ->
        registered? = Subscriptions.register_interest?(socket)
        shared_initial_load? = Subscriptions.shared_initial_load?(socket)

        {value, tracked_deps, interest_keys} =
          SourceLoads.load_or_register(
            socket,
            shared_initial_load?,
            source_id,
            source,
            params,
            component
          )

        if registered? and not shared_initial_load? do
          Subscriptions.register(source_id, interest_keys, source, params)
        end

        socket
        |> State.put_watch(source_id, %{
          assign_name: assign_name,
          assign_names: MapSet.new([assign_name]),
          source: source,
          params: params,
          component: component,
          registered?: registered?,
          interest_keys: interest_keys,
          tracked_deps: tracked_deps
        })
        |> tap(fn _socket ->
          Telemetry.emit([:source, :watch], %{count: 1}, %{
            source_id: source_id,
            node_id: Ids.source_node_id(source_id),
            source: source,
            params: params,
            sharing_partition: Upkeep.Source.sharing_partition(source, params),
            component: component,
            assign_name: assign_name,
            kind: :new,
            registered?: registered?,
            interest_keys: interest_keys
          })
        end)
        |> DAGOperations.put_source(source_id, value, Ids.source_deps(component))
        |> State.put_assign_node(assign_name, Ids.source_node_id(source_id))
        |> Assigns.assign_source_value(assign_name, value, source_id)
    end
  end

  def component(socket, component_id, deps, fun)
      when not is_nil(component_id) and is_list(deps) and is_function(fun, 1) do
    {dep_node_ids, dep_pairs} = DAGOperations.dependency_nodes(socket, deps)
    node_id = Ids.component_node_id(component_id)

    compute = fn node_values ->
      dep_pairs
      |> Map.new(fn {dep, dep_node_id} -> {dep, Map.fetch!(node_values, dep_node_id)} end)
      |> fun.()
    end

    dag =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_component(node_id, dep_node_ids, compute)

    value = Upkeep.DAG.fetch!(dag, node_id)
    dag = Components.put_assign_nodes(dag, component_id, value)

    socket
    |> State.put_dag(dag)
    |> Assigns.assign_component_value(node_id, value)
    |> Assigns.assign_component_assign_values(component_id, value)
  end

  def remove_component(socket, component_id) when not is_nil(component_id) do
    node_id = Ids.component_node_id(component_id)
    removed_node_ids = [node_id | Upkeep.DAG.downstream_ids(State.dag(socket), node_id)]

    socket =
      socket
      |> State.watches()
      |> Enum.filter(fn {_source_id, watch} ->
        Enum.member?(removed_node_ids, Ids.source_node_id(watch.source_id))
      end)
      |> Enum.reduce(socket, fn {source_id, _watch}, socket -> remove_watch(socket, source_id) end)

    dag =
      socket
      |> State.dag()
      |> Upkeep.DAG.remove_subgraph(node_id)

    removed_node_ids
    |> Enum.flat_map(&State.assign_names_for_node(socket, &1))
    |> Enum.reduce(State.put_dag(socket, dag), &State.delete_assign_node(&2, &1))
  end

  def derive(socket, assign_name, deps, fun)
      when is_atom(assign_name) and is_list(deps) and is_function(fun, 1) do
    {dep_node_ids, dep_pairs} = DAGOperations.dependency_nodes(socket, deps)
    node_id = Ids.derived_node_id(assign_name)

    compute = fn node_values ->
      dep_pairs
      |> Map.new(fn {dep, dep_node_id} -> {dep, Map.fetch!(node_values, dep_node_id)} end)
      |> fun.()
    end

    {initial_value, graph_node_id, sharing_metadata} =
      SharedDerived.initial_value(socket, assign_name, dep_node_ids, dep_pairs, fun, compute)

    public_sharing_metadata = Map.delete(sharing_metadata, :compute_fn)

    Telemetry.emit([:derive, :sharing], %{count: 1}, public_sharing_metadata)

    dag =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_derived(node_id, dep_node_ids, compute, initial_value: initial_value)

    value = Upkeep.DAG.fetch!(dag, node_id)

    socket
    |> State.put_dag(dag)
    |> State.put_assign_node(assign_name, node_id)
    |> State.put_derive_sharing(node_id, public_sharing_metadata)
    |> maybe_register_shared_derived_node(node_id, graph_node_id, sharing_metadata)
    |> assign(assign_name, value)
  end

  def unwatch(socket, assign_name) when is_atom(assign_name) do
    socket
    |> State.watches()
    |> Enum.filter(fn {_source_id, watch} -> MapSet.member?(watch.assign_names, assign_name) end)
    |> Enum.reduce(socket, fn {source_id, _watch}, socket ->
      remove_watch_assign(socket, source_id, assign_name)
    end)
  end

  def unwatch(socket, source, params) when is_atom(source) do
    params = normalize_params(params)

    socket
    |> State.watches()
    |> Enum.filter(fn {_source_id, watch} ->
      watch.source == source and watch.params == params
    end)
    |> Enum.reduce(socket, fn {source_id, _watch}, socket -> remove_watch(socket, source_id) end)
  end

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    {value, _tracked_deps} = Upkeep.Source.load(source, normalize_params(params))
    assign(socket, assign_name, value)
  end

  def refresh_matching(socket, event) when is_struct(event) do
    socket
    |> queue_matching(event)
    |> flush_refreshes()
  end

  def graph_snapshot(socket) do
    Snapshot.build(socket)
  end

  def queue_matching(socket, event) when is_struct(event) do
    socket
    |> State.watches()
    |> Enum.reduce(socket, fn {_source_id, watch}, socket ->
      if SourceLoads.reacts_to?(watch, event) do
        Telemetry.emit(
          [:source, :queue],
          %{count: 1},
          Telemetry.watch_metadata(watch, event: event)
        )

        State.queue_refresh(socket, watch.source_id)
      else
        socket
      end
    end)
  end

  def flush_refreshes(socket) do
    {socket, changed_source_nodes} =
      socket
      |> State.pending_refreshes()
      |> Enum.reduce({State.clear_pending_refreshes(socket), []}, fn source_id,
                                                                     {socket, changed} ->
        case Map.fetch(State.watches(socket), source_id) do
          {:ok, watch} -> maybe_refresh(socket, watch, changed)
          :error -> {socket, changed}
        end
      end)

    recompute_derived(socket, changed_source_nodes)
  end

  def notify(event) when is_struct(event), do: Upkeep.notify(event)

  @doc """
  Apply a batch of Graph-pushed values to the LV in one pass.
  Reduces subscriber-side wakeups to one handle_info per shard flush.
  """
  def apply_dag_values(socket, pairs) when is_list(pairs) do
    {socket, changed_nodes, shared_nodes} =
      Enum.reduce(pairs, {socket, [], []}, fn {node_id, value}, {socket, changed, shared} ->
        apply_dag_pair(socket, node_id, value, changed, shared)
      end)

    recompute_derived(socket, changed_nodes, skip: shared_nodes)
  end

  @doc """
  Apply a Graph-pushed value to the LV. Mirrors `maybe_refresh` but skips
  the source.load step — the coordinator already ran it once for everyone.
  """
  def apply_dag_value(socket, source_id, value) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        socket = assign_watch(socket, watch, value)

        {socket, changed?} =
          DAGOperations.put_value(socket, source_id, value, Ids.source_deps(watch.component))

        if changed? do
          recompute_derived(socket, [Ids.source_node_id(source_id)])
        else
          socket
        end

      :error ->
        apply_shared_derived_value(socket, source_id, value)
    end
  end

  defp apply_dag_pair(socket, source_id, value, changed, shared) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        socket = assign_watch(socket, watch, value)

        {socket, changed?} =
          DAGOperations.put_value(socket, source_id, value, Ids.source_deps(watch.component))

        changed =
          if changed? do
            [Ids.source_node_id(source_id) | changed]
          else
            changed
          end

        {socket, changed, shared}

      :error ->
        apply_shared_derived_pair(socket, source_id, value, changed, shared)
    end
  end

  defp apply_shared_derived_pair(socket, graph_node_id, value, changed, shared) do
    case State.local_shared_derived_node(socket, graph_node_id) do
      nil ->
        {socket, changed, shared}

      local_node_id ->
        {socket, changed?} = DAGOperations.put_derived_value(socket, local_node_id, value)
        socket = assign_shared_derived_node(socket, local_node_id, value)

        changed =
          if changed? do
            [local_node_id | changed]
          else
            changed
          end

        {socket, changed, [local_node_id | shared]}
    end
  end

  defp maybe_refresh(socket, watch, changed) do
    {value, tracked_deps} = SourceLoads.load(watch, :refresh)
    watch = SourceLoads.update_watch_deps(watch, tracked_deps)
    socket = State.put_existing_watch(socket, watch.source_id, watch)
    socket = assign_watch(socket, watch, value)

    {socket, changed?} =
      DAGOperations.put_value(socket, watch.source_id, value, Ids.source_deps(watch.component))

    changed =
      if changed? do
        [Ids.source_node_id(watch.source_id) | changed]
      else
        changed
      end

    {socket, changed}
  rescue
    _ -> {socket, changed}
  end

  defp recompute_derived(socket, changed_source_nodes, opts \\ []) do
    DAGOperations.recompute_derived(socket, changed_source_nodes, &remove_watch/2, opts)
  end

  defp assign_watch(socket, watch, value) do
    Enum.reduce(watch.assign_names, socket, fn assign_name, socket ->
      Assigns.assign_source_value(socket, assign_name, value, watch.source_id)
    end)
  end

  defp apply_shared_derived_value(socket, graph_node_id, value) do
    case State.local_shared_derived_node(socket, graph_node_id) do
      nil ->
        socket

      local_node_id ->
        {socket, changed?} = DAGOperations.put_derived_value(socket, local_node_id, value)
        socket = assign_shared_derived_node(socket, local_node_id, value)

        if changed? do
          recompute_derived(socket, [local_node_id])
        else
          socket
        end
    end
  end

  defp assign_shared_derived_node(socket, local_node_id, value) do
    socket
    |> State.assign_names_for_node(local_node_id)
    |> Enum.reduce(socket, fn assign_name, socket ->
      Assigns.assign_derived_value(socket, assign_name, value, local_node_id)
    end)
  end

  defp maybe_register_shared_derived_node(socket, _node_id, nil, _sharing_metadata), do: socket

  defp maybe_register_shared_derived_node(socket, node_id, graph_node_id, sharing_metadata) do
    case Map.fetch(sharing_metadata, :compute_fn) do
      {:ok, compute_fn} ->
        try do
          :ok =
            Subscriptions.register_derived(
              graph_node_id,
              Map.fetch!(sharing_metadata, :graph_dep_node_ids),
              compute_fn
            )
        rescue
          ArgumentError -> :ok
        end

      :error ->
        :ok
    end

    State.put_shared_derived_node(socket, node_id, graph_node_id)
  end

  defp remove_watch(socket, source_id) do
    current_watches = State.watches(socket)

    case Map.fetch(current_watches, source_id) do
      {:ok, watch} ->
        watches = Map.delete(current_watches, source_id)

        if watch.registered? do
          Subscriptions.unregister(source_id)
        end

        Telemetry.emit([:source, :unwatch], %{count: 1}, Telemetry.watch_metadata(watch))

        socket =
          socket
          |> put_watches(watches)
          |> State.delete_pending_refresh(source_id)

        watch.assign_names
        |> Enum.reduce(socket, &State.delete_assign_node(&2, &1))
        |> DAGOperations.remove_source(source_id)

      :error ->
        socket
    end
  end

  defp remove_watch_assign(socket, source_id, assign_name) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        assign_names = MapSet.delete(watch.assign_names, assign_name)

        if Enum.empty?(assign_names) do
          remove_watch(socket, source_id)
        else
          Telemetry.emit(
            [:source, :unwatch],
            %{count: 1},
            Telemetry.watch_metadata(watch, assign_name, :alias)
          )

          socket
          |> State.put_existing_watch(source_id, %{watch | assign_names: assign_names})
          |> State.delete_assign_node(assign_name)
        end

      :error ->
        socket
    end
  end

  defp put_watches(socket, watches) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp primary_assign_name(watch) do
    watch.assign_name || Enum.at(watch.assign_names, 0)
  end

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params
end
