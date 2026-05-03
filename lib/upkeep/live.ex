defmodule Upkeep.Live do
  @moduledoc """
  LiveView integration for watching Upkeep sources.
  """

  import Phoenix.Component, only: [assign: 3]

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor

  defmacro __using__(_opts) do
    quote do
      import Upkeep.Live, only: [watch: 4, derive: 4, unwatch: 2, unwatch: 3, refresh: 4]

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

  def watch(socket, assign_name, source, params) when is_atom(assign_name) do
    params = normalize_params(params)
    source_id = Source.source_id(source, params)

    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        socket
        |> put_watch_assign(source_id, assign_name)
        |> put_assign_node(assign_name, source_node_id(source_id))
        |> assign(assign_name, Map.fetch!(socket.assigns, primary_assign_name(watch)))

      :error ->
        value = source.load(params)
        interest_keys = source.__upkeep_interest_keys__(params)

        join_interest(interest_keys, assign_name, source)

        socket
        |> put_watch(source_id, %{
          assign_name: assign_name,
          assign_names: MapSet.new([assign_name]),
          source: source,
          params: params,
          interest_keys: interest_keys
        })
        |> put_dag_source(source_id, value)
        |> put_assign_node(assign_name, source_node_id(source_id))
        |> assign(assign_name, value)
    end
  end

  def derive(socket, assign_name, deps, fun)
      when is_atom(assign_name) and is_list(deps) and is_function(fun, 1) do
    {dep_node_ids, dep_pairs} =
      deps
      |> Enum.map(fn dep ->
        node_id =
          Map.get(assign_nodes(socket), dep) ||
            raise ArgumentError, "unknown Upkeep dependency assign #{inspect(dep)}"

        {node_id, {dep, node_id}}
      end)
      |> Enum.unzip()

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
    source_id = Source.source_id(source, params)
    remove_watch(socket, source_id)
  end

  def refresh(socket, assign_name, source, params) when is_atom(assign_name) do
    assign(socket, assign_name, source.load(params))
  end

  def refresh_matching(socket, event) when is_struct(event) do
    socket
    |> queue_matching(event)
    |> flush_refreshes()
  end

  def queue_matching(socket, event) when is_struct(event) do
    socket
    |> watches()
    |> Enum.reduce(socket, fn {_source_id, watch}, socket ->
      if watch.source.reacts_to?(event, watch.params) do
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
    value = watch.source.load(watch.params)

    socket = assign_watch(socket, watch, value)
    {socket, changed?} = put_dag_value(socket, watch.source_id, value)

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
      socket
      |> dag()
      |> Upkeep.DAG.recompute(changed_source_nodes)

    socket
    |> put_dag(dag)
    |> assign_derived_nodes(changed_derived_nodes)
  end

  defp assign_derived_nodes(socket, node_ids) do
    Enum.reduce(node_ids, socket, fn node_id, socket ->
      value = Upkeep.DAG.fetch!(dag(socket), node_id)

      socket
      |> assign_names_for_node(node_id)
      |> Enum.reduce(socket, fn assign_name, socket -> assign(socket, assign_name, value) end)
    end)
  end

  defp assign_watch(socket, watch, value) do
    Enum.reduce(watch.assign_names, socket, fn assign_name, socket ->
      assign(socket, assign_name, value)
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
    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        leave_interest(watch.interest_keys)

        private = socket.private || %{}
        watches = Map.delete(Map.get(private, :upkeep_watches, %{}), source_id)

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

  defp remove_watch_assign(socket, source_id, assign_name) do
    case Map.fetch(watches(socket), source_id) do
      {:ok, watch} ->
        assign_names = MapSet.delete(watch.assign_names, assign_name)

        if Enum.empty?(assign_names) do
          remove_watch(socket, source_id)
        else
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

  defp put_dag_source(socket, source_id, value) do
    {dag, _changed?} =
      socket
      |> dag()
      |> Upkeep.DAG.put_source(source_node_id(source_id), value)

    put_dag(socket, dag)
  end

  defp put_dag_value(socket, source_id, value) do
    {dag, changed?} =
      socket
      |> dag()
      |> Upkeep.DAG.put_source(source_node_id(source_id), value)

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

  defp source_node_id(source_id), do: {:source, source_id}
  defp derived_node_id(assign_name), do: {:derived, assign_name}

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

  defp normalize_params(params) when is_list(params), do: Map.new(params)
  defp normalize_params(params) when is_map(params), do: params
end
