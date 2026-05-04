defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.
  """

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Live.{Assigns, Components, Ids, State, Subscriptions, Telemetry}

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
      def handle_info({:dag_value, source_id, value}, socket) do
        {:noreply, Upkeep.Live.apply_dag_value(socket, source_id, value)}
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
        {value, tracked_deps} = load_source(source, params, source_id, component, :watch)

        interest_keys =
          (source.__upkeep_interest_keys__(params) ++
             Upkeep.Source.deps_interest_keys(tracked_deps))
          |> Enum.uniq()

        registered? = Subscriptions.register_interest?(socket)

        if registered? do
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
            component: component,
            assign_name: assign_name,
            kind: :new,
            registered?: registered?,
            interest_keys: interest_keys
          })
        end)
        |> put_dag_source(source_id, value, Ids.source_deps(component))
        |> State.put_assign_node(assign_name, Ids.source_node_id(source_id))
        |> Assigns.assign_source_value(assign_name, value, source_id)
    end
  end

  def component(socket, component_id, deps, fun)
      when not is_nil(component_id) and is_list(deps) and is_function(fun, 1) do
    {dep_node_ids, dep_pairs} = dependency_nodes(socket, deps)
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
    {dep_node_ids, dep_pairs} = dependency_nodes(socket, deps)
    node_id = Ids.derived_node_id(assign_name)

    compute = fn node_values ->
      dep_pairs
      |> Map.new(fn {dep, dep_node_id} -> {dep, Map.fetch!(node_values, dep_node_id)} end)
      |> fun.()
    end

    dag =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_derived(node_id, dep_node_ids, compute)

    value = Upkeep.DAG.fetch!(dag, node_id)

    socket
    |> State.put_dag(dag)
    |> State.put_assign_node(assign_name, node_id)
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
    %{
      dag: Upkeep.DAG.snapshot(State.dag(socket)),
      assigns: assign_snapshot(socket),
      watches: watch_snapshot(socket),
      pending_refreshes: pending_refresh_snapshot(socket)
    }
  end

  def queue_matching(socket, event) when is_struct(event) do
    socket
    |> State.watches()
    |> Enum.reduce(socket, fn {_source_id, watch}, socket ->
      if reacts_to?(watch, event) do
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
  Apply a NodeDAG-pushed value to the LV. Mirrors `maybe_refresh` but skips
  the source.load step — the coordinator already ran it once for everyone.
  """
  def apply_dag_value(socket, source_id, value) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        socket = assign_watch(socket, watch, value)

        {socket, changed?} =
          put_dag_value(socket, source_id, value, Ids.source_deps(watch.component))

        if changed? do
          recompute_derived(socket, [Ids.source_node_id(source_id)])
        else
          socket
        end

      :error ->
        socket
    end
  end

  defp maybe_refresh(socket, watch, changed) do
    {value, tracked_deps} = load_source(watch, :refresh)
    watch = update_watch_deps(watch, tracked_deps)
    socket = State.put_existing_watch(socket, watch.source_id, watch)
    socket = assign_watch(socket, watch, value)

    {socket, changed?} =
      put_dag_value(socket, watch.source_id, value, Ids.source_deps(watch.component))

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

  defp recompute_derived(socket, []), do: socket

  defp recompute_derived(socket, changed_source_nodes) do
    {dag, changed_derived_nodes, _recomputed_nodes} =
      Telemetry.span([:dag, :recompute], %{changed_source_nodes: changed_source_nodes}, fn ->
        socket
        |> State.dag()
        |> Upkeep.DAG.recompute(changed_source_nodes)
        |> then(fn {_dag, changed_derived_nodes, recomputed_nodes} = result ->
          {result,
           %{
             changed_derived_nodes: changed_derived_nodes,
             recomputed_nodes: recomputed_nodes,
             changed_count: length(changed_derived_nodes),
             recomputed_count: length(recomputed_nodes)
           }}
        end)
      end)

    socket
    |> State.put_dag(dag)
    |> remove_changed_component_watches(changed_derived_nodes)
    |> assign_derived_nodes(changed_derived_nodes)
  end

  defp remove_changed_component_watches(socket, node_ids) do
    node_ids
    |> Components.changed_component_ids()
    |> Enum.reduce(socket, fn component_id, socket ->
      socket
      |> State.watches()
      |> Enum.filter(fn {_source_id, watch} -> watch.component == component_id end)
      |> Enum.reduce(socket, fn {source_id, _watch}, socket -> remove_watch(socket, source_id) end)
    end)
  end

  defp assign_derived_nodes(socket, node_ids) do
    Enum.reduce(node_ids, socket, fn node_id, socket ->
      value = Upkeep.DAG.fetch!(State.dag(socket), node_id)
      assign_node_value(socket, node_id, value)
    end)
  end

  defp assign_node_value(socket, {:component, _component_id} = node_id, value) do
    Assigns.assign_component_value(socket, node_id, value)
  end

  defp assign_node_value(socket, node_id, value) do
    socket
    |> State.assign_names_for_node(node_id)
    |> Enum.reduce(socket, fn assign_name, socket ->
      Assigns.assign_derived_value(socket, assign_name, value, node_id)
    end)
  end

  defp assign_watch(socket, watch, value) do
    Enum.reduce(watch.assign_names, socket, fn assign_name, socket ->
      Assigns.assign_source_value(socket, assign_name, value, watch.source_id)
    end)
  end

  defp load_source(watch, reason) do
    load_source(watch.source, watch.params, watch.source_id, watch.component, reason)
  end

  defp load_source(source, params, source_id, component, reason) do
    Telemetry.span(
      [:source, :reload],
      Telemetry.source_metadata(source, params, source_id, component, reason),
      fn ->
        {value, tracked_deps} = Upkeep.Source.load(source, params)
        {{value, tracked_deps}, %{changed?: nil, tracked_deps: length(tracked_deps)}}
      end
    )
  end

  defp reacts_to?(watch, event) do
    Upkeep.Source.deps_react_to?(Map.get(watch, :tracked_deps, []), event) or
      watch.source.reacts_to?(event, watch.params)
  end

  defp update_watch_deps(watch, tracked_deps) do
    interest_keys =
      (watch.source.__upkeep_interest_keys__(watch.params) ++
         Upkeep.Source.deps_interest_keys(tracked_deps))
      |> Enum.uniq()

    %{watch | interest_keys: interest_keys, tracked_deps: tracked_deps}
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
        |> remove_dag_source(source_id)

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

  defp put_dag_source(socket, source_id, value, deps) do
    {dag, _changed?} =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_source(Ids.source_node_id(source_id), value, deps)

    State.put_dag(socket, dag)
  end

  defp put_dag_value(socket, source_id, value, deps) do
    {dag, changed?} =
      socket
      |> State.dag()
      |> Upkeep.DAG.put_source(Ids.source_node_id(source_id), value, deps)

    {State.put_dag(socket, dag), changed?}
  end

  defp remove_dag_source(socket, source_id) do
    source_node_id = Ids.source_node_id(source_id)

    removed_node_ids = [
      source_node_id | Upkeep.DAG.downstream_ids(State.dag(socket), source_node_id)
    ]

    socket =
      removed_node_ids
      |> Enum.flat_map(&State.assign_names_for_node(socket, &1))
      |> Enum.reduce(socket, &State.delete_assign_node(&2, &1))

    dag =
      socket
      |> State.dag()
      |> Upkeep.DAG.remove_subgraph(source_node_id)

    State.put_dag(socket, dag)
  end

  defp dependency_nodes(socket, deps) do
    deps
    |> Enum.map(fn dep ->
      node_id =
        Map.get(State.assign_nodes(socket), dep) ||
          raise ArgumentError, "unknown Upkeep dependency assign #{inspect(dep)}"

      {node_id, {dep, node_id}}
    end)
    |> Enum.unzip()
  end

  defp assign_snapshot(socket) do
    socket
    |> State.assign_nodes()
    |> Enum.map(fn {assign_name, node_id} -> %{assign: assign_name, node_id: node_id} end)
    |> sort_maps_by(:assign)
  end

  defp watch_snapshot(socket) do
    socket
    |> State.watches()
    |> Enum.map(fn {source_id, watch} ->
      %{
        source_id: source_id,
        node_id: Ids.source_node_id(source_id),
        source: watch.source,
        params: watch.params,
        component: watch.component,
        assign_names: watch.assign_names |> MapSet.to_list() |> Telemetry.sort_terms(),
        interest_keys: Telemetry.sort_terms(watch.interest_keys)
      }
    end)
    |> sort_maps_by(:source_id)
  end

  defp pending_refresh_snapshot(socket) do
    socket
    |> State.pending_refreshes()
    |> MapSet.to_list()
    |> Telemetry.sort_terms()
  end

  defp put_watches(socket, watches) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp primary_assign_name(watch) do
    watch.assign_name || Enum.at(watch.assign_names, 0)
  end

  defp sort_maps_by(maps, key), do: Enum.sort_by(maps, &inspect(Map.fetch!(&1, key)))

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params
end
