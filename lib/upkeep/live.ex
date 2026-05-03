defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.
  """

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor

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
      def handle_info({:upkeep_event, event}, socket) do
        socket =
          socket
          |> Upkeep.Live.queue_matching(event)
          |> Upkeep.Live.flush_refreshes()

        {:noreply, socket}
      end

      defoverridable handle_info: 2
    end
  end

  def watch(socket, assign_name, source, params, opts \\ []) when is_atom(assign_name) do
    params = normalize_params(params)
    component = Keyword.get(opts, :under)
    source_id = scoped_source_id(source, params, component)

    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        emit([:source, :watch], %{count: 1}, watch_metadata(watch, assign_name, :alias))

        socket
        |> put_watch_assign(source_id, assign_name)
        |> put_assign_node(assign_name, source_node_id(source_id))
        |> assign(assign_name, Map.fetch!(socket.assigns, primary_assign_name(watch)))

      :error ->
        value = load_source(source, params, source_id, component, :watch)
        interest_keys = source.__upkeep_interest_keys__(params)

        registered? = register_interest?(socket)

        if registered? do
          join_interest(interest_keys, assign_name, source)
        end

        socket
        |> put_watch(source_id, %{
          assign_name: assign_name,
          assign_names: MapSet.new([assign_name]),
          source: source,
          params: params,
          component: component,
          registered?: registered?,
          interest_keys: interest_keys
        })
        |> tap(fn _socket ->
          emit([:source, :watch], %{count: 1}, %{
            source_id: source_id,
            node_id: source_node_id(source_id),
            source: source,
            params: params,
            component: component,
            assign_name: assign_name,
            kind: :new,
            registered?: registered?,
            interest_keys: interest_keys
          })
        end)
        |> put_dag_source(source_id, value, source_deps(component))
        |> put_assign_node(assign_name, source_node_id(source_id))
        |> assign_source_value(assign_name, value, source_id)
    end
  end

  def component(socket, component_name, deps, fun)
      when is_atom(component_name) and is_list(deps) and is_function(fun, 1) do
    {dep_node_ids, dep_pairs} = dependency_nodes(socket, deps)
    node_id = component_node_id(component_name)

    compute = fn node_values ->
      dep_pairs
      |> Map.new(fn {dep, dep_node_id} -> {dep, Map.fetch!(node_values, dep_node_id)} end)
      |> fun.()
    end

    dag =
      socket
      |> dag()
      |> Upkeep.DAG.put_component(node_id, dep_node_ids, compute)

    put_dag(socket, dag)
  end

  def remove_component(socket, component_name) when is_atom(component_name) do
    node_id = component_node_id(component_name)
    removed_node_ids = [node_id | Upkeep.DAG.downstream_ids(dag(socket), node_id)]

    socket =
      socket
      |> watches()
      |> Enum.filter(fn {_source_id, watch} ->
        Enum.member?(removed_node_ids, source_node_id(watch.source_id))
      end)
      |> Enum.reduce(socket, fn {source_id, _watch}, socket -> remove_watch(socket, source_id) end)

    dag =
      socket
      |> dag()
      |> Upkeep.DAG.remove_subgraph(node_id)

    removed_node_ids
    |> Enum.flat_map(&assign_names_for_node(socket, &1))
    |> Enum.reduce(put_dag(socket, dag), &delete_assign_node(&2, &1))
  end

  def derive(socket, assign_name, deps, fun)
      when is_atom(assign_name) and is_list(deps) and is_function(fun, 1) do
    {dep_node_ids, dep_pairs} = dependency_nodes(socket, deps)
    node_id = derived_node_id(assign_name)

    compute = fn node_values ->
      dep_pairs
      |> Map.new(fn {dep, dep_node_id} -> {dep, Map.fetch!(node_values, dep_node_id)} end)
      |> fun.()
    end

    dag =
      socket
      |> dag()
      |> Upkeep.DAG.put_derived(node_id, dep_node_ids, compute)

    value = Upkeep.DAG.fetch!(dag, node_id)

    socket
    |> put_dag(dag)
    |> put_assign_node(assign_name, node_id)
    |> assign(assign_name, value)
  end

  def unwatch(socket, assign_name) when is_atom(assign_name) do
    socket
    |> watches()
    |> Enum.filter(fn {_source_id, watch} -> MapSet.member?(watch.assign_names, assign_name) end)
    |> Enum.reduce(socket, fn {source_id, _watch}, socket ->
      remove_watch_assign(socket, source_id, assign_name)
    end)
  end

  def unwatch(socket, source, params) when is_atom(source) do
    params = normalize_params(params)

    socket
    |> watches()
    |> Enum.filter(fn {_source_id, watch} ->
      watch.source == source and watch.params == params
    end)
    |> Enum.reduce(socket, fn {source_id, _watch}, socket -> remove_watch(socket, source_id) end)
  end

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    assign(socket, assign_name, source.load(params))
  end

  def refresh_matching(socket, event) when is_struct(event) do
    socket
    |> queue_matching(event)
    |> flush_refreshes()
  end

  def graph_snapshot(socket) do
    %{
      dag: Upkeep.DAG.snapshot(dag(socket)),
      assigns: assign_snapshot(socket),
      watches: watch_snapshot(socket),
      pending_refreshes: pending_refresh_snapshot(socket)
    }
  end

  def queue_matching(socket, event) when is_struct(event) do
    socket
    |> watches()
    |> Enum.reduce(socket, fn {_source_id, watch}, socket ->
      if watch.source.reacts_to?(event, watch.params) do
        emit([:source, :queue], %{count: 1}, watch_metadata(watch, event: event))
        queue_refresh(socket, watch.source_id)
      else
        socket
      end
    end)
  end

  def flush_refreshes(socket) do
    {socket, changed_source_nodes} =
      socket
      |> pending_refreshes()
      |> Enum.reduce({clear_pending_refreshes(socket), []}, fn source_id, {socket, changed} ->
        case Map.fetch(watches(socket), source_id) do
          {:ok, watch} -> maybe_refresh(socket, watch, changed)
          :error -> {socket, changed}
        end
      end)

    recompute_derived(socket, changed_source_nodes)
  end

  defp maybe_refresh(socket, watch, changed) do
    value = load_source(watch, :refresh)

    socket = assign_watch(socket, watch, value)

    {socket, changed?} =
      put_dag_value(socket, watch.source_id, value, source_deps(watch.component))

    changed =
      if changed? do
        [source_node_id(watch.source_id) | changed]
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
      span([:dag, :recompute], %{changed_source_nodes: changed_source_nodes}, fn ->
        socket
        |> dag()
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
    |> put_dag(dag)
    |> assign_derived_nodes(changed_derived_nodes)
  end

  defp assign_derived_nodes(socket, node_ids) do
    Enum.reduce(node_ids, socket, fn node_id, socket ->
      value = Upkeep.DAG.fetch!(dag(socket), node_id)

      socket
      |> assign_node_value(node_id, value)
    end)
  end

  defp assign_node_value(socket, {:component, _component_name} = node_id, value) do
    assign_component_value(socket, node_id, value)
  end

  defp assign_node_value(socket, node_id, value) do
    socket
    |> assign_names_for_node(node_id)
    |> Enum.reduce(socket, fn assign_name, socket ->
      assign_derived_value(socket, assign_name, value, node_id)
    end)
  end

  defp assign_watch(socket, watch, value) do
    Enum.reduce(watch.assign_names, socket, fn assign_name, socket ->
      assign_source_value(socket, assign_name, value, watch.source_id)
    end)
  end

  def notify(event) when is_struct(event), do: Upkeep.notify(event)

  defp join_interest(interest_keys, assign_name, source) do
    for key <- interest_keys do
      :ok =
        Group.join(@supervisor, Source.group_key(key), %{
          assign: assign_name,
          source: inspect(source)
        })
    end
  end

  defp load_source(watch, reason) do
    load_source(watch.source, watch.params, watch.source_id, watch.component, reason)
  end

  defp load_source(source, params, source_id, component, reason) do
    span([:source, :reload], source_metadata(source, params, source_id, component, reason), fn ->
      value = source.load(params)
      {value, %{changed?: nil}}
    end)
  end

  defp put_watch(socket, source_id, watch) do
    private = socket.private || %{}
    watch = Map.put(watch, :source_id, source_id)
    watches = Map.put(Map.get(private, :upkeep_watches, %{}), source_id, watch)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp put_watch_assign(socket, source_id, assign_name) do
    private = socket.private || %{}

    watches =
      Map.update!(Map.get(private, :upkeep_watches, %{}), source_id, fn watch ->
        Map.update!(watch, :assign_names, &MapSet.put(&1, assign_name))
      end)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp remove_watch(socket, source_id) do
    current_watches = watches(socket)

    case Map.fetch(current_watches, source_id) do
      {:ok, watch} ->
        watches = Map.delete(current_watches, source_id)

        if watch.registered? do
          leave_interest(unused_interest_keys(watch.interest_keys, watches))
        end

        emit([:source, :unwatch], %{count: 1}, watch_metadata(watch))

        private = socket.private || %{}

        pending =
          MapSet.delete(Map.get(private, :upkeep_pending_refreshes, MapSet.new()), source_id)

        socket =
          %{
            socket
            | private:
                private
                |> Map.put(:upkeep_watches, watches)
                |> Map.put(:upkeep_pending_refreshes, pending)
          }

        watch.assign_names
        |> Enum.reduce(socket, &delete_assign_node(&2, &1))
        |> remove_dag_source(source_id)

      :error ->
        socket
    end
  end

  defp unused_interest_keys(interest_keys, watches) do
    remaining_keys =
      watches
      |> Map.values()
      |> Enum.flat_map(& &1.interest_keys)
      |> MapSet.new()

    Enum.reject(interest_keys, &MapSet.member?(remaining_keys, &1))
  end

  defp remove_watch_assign(socket, source_id, assign_name) do
    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        assign_names = MapSet.delete(watch.assign_names, assign_name)

        if Enum.empty?(assign_names) do
          remove_watch(socket, source_id)
        else
          emit([:source, :unwatch], %{count: 1}, watch_metadata(watch, assign_name, :alias))

          socket
          |> put_existing_watch(source_id, %{watch | assign_names: assign_names})
          |> delete_assign_node(assign_name)
        end

      :error ->
        socket
    end
  end

  defp put_existing_watch(socket, source_id, watch) do
    private = socket.private || %{}
    watches = Map.put(Map.get(private, :upkeep_watches, %{}), source_id, watch)

    %{socket | private: Map.put(private, :upkeep_watches, watches)}
  end

  defp leave_interest(interest_keys) do
    for key <- interest_keys do
      case Group.leave(@supervisor, Source.group_key(key)) do
        :ok -> :ok
        {:error, :not_in_group} -> :ok
      end
    end

    :ok
  end

  defp put_dag_source(socket, source_id, value, deps) do
    {dag, _changed?} =
      socket
      |> dag()
      |> Upkeep.DAG.put_source(source_node_id(source_id), value, deps)

    put_dag(socket, dag)
  end

  defp put_dag_value(socket, source_id, value, deps) do
    {dag, changed?} =
      socket
      |> dag()
      |> Upkeep.DAG.put_source(source_node_id(source_id), value, deps)

    {put_dag(socket, dag), changed?}
  end

  defp remove_dag_source(socket, source_id) do
    source_node_id = source_node_id(source_id)
    removed_node_ids = [source_node_id | Upkeep.DAG.downstream_ids(dag(socket), source_node_id)]

    socket =
      removed_node_ids
      |> Enum.flat_map(&assign_names_for_node(socket, &1))
      |> Enum.reduce(socket, &delete_assign_node(&2, &1))

    dag =
      socket
      |> dag()
      |> Upkeep.DAG.remove_subgraph(source_node_id)

    put_dag(socket, dag)
  end

  defp put_dag(socket, dag) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_dag, dag)}
  end

  defp put_assign_node(socket, assign_name, node_id) do
    private = socket.private || %{}
    assign_nodes = Map.put(Map.get(private, :upkeep_assign_nodes, %{}), assign_name, node_id)

    %{socket | private: Map.put(private, :upkeep_assign_nodes, assign_nodes)}
  end

  defp delete_assign_node(socket, assign_name) do
    private = socket.private || %{}
    assign_nodes = Map.delete(Map.get(private, :upkeep_assign_nodes, %{}), assign_name)

    %{socket | private: Map.put(private, :upkeep_assign_nodes, assign_nodes)}
  end

  defp dag(socket) do
    case socket.private do
      %{upkeep_dag: dag} -> dag
      _ -> Upkeep.DAG.new()
    end
  end

  defp assign_nodes(socket) do
    case socket.private do
      %{upkeep_assign_nodes: assign_nodes} -> assign_nodes
      _ -> %{}
    end
  end

  defp assign_names_for_node(socket, node_id) do
    socket
    |> assign_nodes()
    |> Enum.filter(fn {_assign_name, assigned_node_id} -> assigned_node_id == node_id end)
    |> Enum.map(fn {assign_name, _node_id} -> assign_name end)
  end

  defp assign_snapshot(socket) do
    socket
    |> assign_nodes()
    |> Enum.map(fn {assign_name, node_id} -> %{assign: assign_name, node_id: node_id} end)
    |> sort_maps_by(:assign)
  end

  defp watch_snapshot(socket) do
    socket
    |> watches()
    |> Enum.map(fn {source_id, watch} ->
      %{
        source_id: source_id,
        node_id: source_node_id(source_id),
        source: watch.source,
        params: watch.params,
        component: watch.component,
        assign_names: watch.assign_names |> MapSet.to_list() |> sort_terms(),
        interest_keys: sort_terms(watch.interest_keys)
      }
    end)
    |> sort_maps_by(:source_id)
  end

  defp pending_refresh_snapshot(socket) do
    socket
    |> pending_refreshes()
    |> MapSet.to_list()
    |> sort_terms()
  end

  defp source_node_id(source_id), do: {:source, source_id}
  defp derived_node_id(assign_name), do: {:derived, assign_name}
  defp component_node_id(component_name), do: {:component, component_name}

  defp source_deps(nil), do: []
  defp source_deps(component), do: [component_node_id(component)]

  defp scoped_source_id(source, params, nil), do: Source.source_id(source, params)

  defp scoped_source_id(source, params, component) when is_atom(component) do
    {:scoped, component, Source.source_id(source, params)}
  end

  defp dependency_nodes(socket, deps) do
    deps
    |> Enum.map(fn dep ->
      node_id =
        Map.get(assign_nodes(socket), dep) ||
          raise ArgumentError, "unknown Upkeep dependency assign #{inspect(dep)}"

      {node_id, {dep, node_id}}
    end)
    |> Enum.unzip()
  end

  defp queue_refresh(socket, source_id) do
    private = socket.private || %{}
    pending = MapSet.put(Map.get(private, :upkeep_pending_refreshes, MapSet.new()), source_id)

    %{socket | private: Map.put(private, :upkeep_pending_refreshes, pending)}
  end

  defp pending_refreshes(socket) do
    case socket.private do
      %{upkeep_pending_refreshes: pending} -> pending
      _ -> MapSet.new()
    end
  end

  defp clear_pending_refreshes(socket) do
    private = socket.private || %{}
    %{socket | private: Map.put(private, :upkeep_pending_refreshes, MapSet.new())}
  end

  defp primary_assign_name(watch) do
    watch.assign_name || Enum.at(watch.assign_names, 0)
  end

  defp watches(socket) do
    case socket.private do
      %{upkeep_watches: watches} -> watches
      _ -> %{}
    end
  end

  defp sort_maps_by(maps, key), do: Enum.sort_by(maps, &inspect(Map.fetch!(&1, key)))
  defp sort_terms(terms), do: Enum.sort_by(terms, &inspect/1)

  defp assign_source_value(socket, assign_name, value, source_id) do
    emit([:live, :assign], %{count: 1}, %{
      assign: assign_name,
      node_id: source_node_id(source_id),
      source_id: source_id,
      kind: :source
    })

    assign(socket, assign_name, value)
  end

  defp assign_derived_value(socket, assign_name, value, node_id) do
    emit([:live, :assign], %{count: 1}, %{
      assign: assign_name,
      node_id: node_id,
      kind: :derived
    })

    assign(socket, assign_name, value)
  end

  defp assign_component_value(socket, _node_id, value) when is_map(value) do
    Enum.reduce(value, socket, fn
      {assign_name, assign_value}, socket when is_atom(assign_name) ->
        assign(socket, assign_name, assign_value)

      {_assign_name, _assign_value}, socket ->
        socket
    end)
  end

  defp assign_component_value(socket, _node_id, _value), do: socket

  defp watch_metadata(watch), do: watch_metadata(watch, nil, :remove)

  defp watch_metadata(watch, opts) when is_list(opts) do
    Map.merge(watch_metadata(watch), Map.new(opts))
  end

  defp watch_metadata(watch, assign_name, kind) do
    %{
      source_id: watch.source_id,
      node_id: source_node_id(watch.source_id),
      source: watch.source,
      params: watch.params,
      component: watch.component,
      assign_name: assign_name,
      assign_names: watch.assign_names |> MapSet.to_list() |> sort_terms(),
      kind: kind,
      registered?: watch.registered?,
      interest_keys: watch.interest_keys
    }
  end

  defp source_metadata(source, params, source_id, component, reason) do
    %{
      source_id: source_id,
      node_id: source_node_id(source_id),
      source: source,
      params: params,
      component: component,
      reason: reason
    }
  end

  defp emit(event, measurements, metadata) do
    :telemetry.execute([:upkeep | event], measurements, metadata)
  end

  defp span(event, metadata, fun) do
    :telemetry.span([:upkeep | event], metadata, fn ->
      {result, stop_metadata} = fun.()
      {result, Map.merge(metadata, stop_metadata)}
    end)
  end

  defp register_interest?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: true

  defp register_interest?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params
end
