defmodule Upkeep.Runtime.Execution.Shared do
  @moduledoc false

  alias Upkeep.DAG.Store
  alias Upkeep.Runtime.{ScopeCapture, State, Subscriptions}

  def initial_value(
        socket,
        assign_name,
        dep_node_ids,
        dep_pairs,
        fun,
        compute,
        source_location \\ nil
      ) do
    analysis = ScopeCapture.analyze(fun)

    :ok =
      ScopeCapture.apply_policy(analysis, %{
        assign_name: assign_name,
        source_location: source_location
      })

    metadata = sharing_metadata(socket, assign_name, dep_node_ids)

    case shared_context(socket, assign_name, dep_node_ids, dep_pairs, fun, analysis) do
      {:ok, context} ->
        context
        |> register_shared_derived()
        |> case do
          {:ok, value} ->
            {value, context.graph_node_id,
             Map.merge(metadata, %{
               result: :shared,
               reason: :external_fun,
               graph_node_id: context.graph_node_id,
               dep_graph_node_ids: context.dep_graph_node_ids,
               fun_identity: context.fun_identity,
               candidate_shareable_nodes: dep_node_ids
             })}

          {:error, reason} ->
            raise_compute_error(reason)
        end

      {:error, reason, extra} ->
        {compute_initial_value(socket, dep_node_ids, compute), nil,
         metadata
         |> Map.merge(%{result: :local, reason: reason})
         |> Map.merge(extra)}
    end
  end

  def sharing_plan(_socket, _dep_node_ids, metadata) when is_map(metadata) do
    %{
      final_result: Map.get(metadata, :result),
      final_reason: Map.get(metadata, :reason),
      roots: Map.get(metadata, :dep_node_ids, []),
      candidate_shareable_nodes: Map.get(metadata, :candidate_shareable_nodes, []),
      candidate_shareable_subgraphs: [],
      largest_shareable_subgraphs: [],
      boundaries: Map.get(metadata, :boundaries, [])
    }
  end

  defp sharing_metadata(socket, assign_name, dep_node_ids) do
    %{
      assign_name: assign_name,
      view: Map.get(socket, :view),
      dep_node_ids: dep_node_ids,
      implicit_scope: ScopeCapture.implicit_scope_metadata(socket, dep_node_ids)
    }
  end

  defp shared_context(socket, assign_name, dep_node_ids, dep_pairs, fun, analysis) do
    with :ok <- shareable_socket?(socket),
         {:ok, fun_identity} <- shareable_fun_identity(analysis),
         {:ok, deps} <- shareable_dependencies(socket, dep_node_ids),
         :ok <- shared_enough_dependencies?(deps) do
      :ok = register_source_dependencies(deps)

      dep_graph_node_ids = Enum.map(deps, & &1.graph_node_id)
      local_to_graph = Map.new(deps, &{&1.local_node_id, &1.graph_node_id})

      graph_node_id =
        {:derived, Map.get(socket, :view), assign_name, dep_graph_node_ids, fun_identity}

      graph_compute = graph_compute_fun(dep_pairs, local_to_graph, fun)

      {:ok,
       %{
         graph_node_id: graph_node_id,
         dep_graph_node_ids: dep_graph_node_ids,
         dep_values: Map.new(deps, &{&1.graph_node_id, &1.value}),
         compute: graph_compute,
         fun_identity: fun_identity,
         metadata: %{
           assign_name: assign_name,
           view: Map.get(socket, :view),
           dep_graph_node_ids: dep_graph_node_ids,
           fun_identity: fun_identity
         }
       }}
    end
  end

  defp shareable_socket?(socket) do
    if Subscriptions.shared_initial_load?(socket) do
      :ok
    else
      {:error, :not_connected, %{boundaries: [%{reason: :not_connected}]}}
    end
  end

  defp shareable_fun_identity({:external, fun_identity}), do: {:ok, fun_identity}

  defp shareable_fun_identity({:captured_scope, _fun_identity, scope_capture}) do
    {:error, :captured_scope,
     %{boundaries: [%{reason: :captured_scope, scope_capture: scope_capture}]}}
  end

  defp shareable_fun_identity(_analysis) do
    {:error, :captured_function, %{boundaries: [%{reason: :captured_function}]}}
  end

  defp shareable_dependencies(socket, dep_node_ids) do
    dep_node_ids
    |> Enum.reduce_while({:ok, []}, fn dep_node_id, {:ok, deps} ->
      case shareable_dependency(socket, dep_node_id) do
        {:ok, dep} -> {:cont, {:ok, [dep | deps]}}
        {:error, reason, extra} -> {:halt, {:error, reason, extra}}
      end
    end)
    |> case do
      {:ok, deps} -> {:ok, Enum.reverse(deps)}
      other -> other
    end
  end

  defp shareable_dependency(_socket, {:source, {:scoped, _component_id, _source_id}} = node_id) do
    unsupported_dependency(node_id, :component_scoped_source)
  end

  defp shareable_dependency(socket, {:source, source_id} = local_node_id) do
    case Map.fetch(State.watches(socket), source_id) do
      {:ok, watch} ->
        {:ok,
         %{
           local_node_id: local_node_id,
           graph_node_id: source_id,
           value: Store.fetch!(State.store(socket), local_node_id),
           subscriber_count: Subscriptions.node_member_count(source_id),
           source_registration: {source_id, watch.surface, watch.instance}
         }}

      :error ->
        unsupported_dependency(local_node_id, :unwatched_source)
    end
  end

  defp shareable_dependency(socket, {:derived, _assign_name} = local_node_id) do
    case State.shared_derived_graph_node(socket, local_node_id) do
      {:ok, graph_node_id} ->
        {:ok,
         %{
           local_node_id: local_node_id,
           graph_node_id: graph_node_id,
           value: Store.fetch!(State.store(socket), local_node_id),
           subscriber_count: Subscriptions.node_member_count(graph_node_id)
         }}

      :error ->
        unsupported_dependency(local_node_id, :local_derived_dependency)
    end
  end

  defp shareable_dependency(_socket, node_id),
    do: unsupported_dependency(node_id, :local_dependency)

  defp unsupported_dependency(node_id, reason) do
    {:error, reason, %{boundaries: [%{node_id: node_id, reason: reason}]}}
  end

  defp shared_enough_dependencies?([]) do
    {:error, :no_shared_dependencies, %{boundaries: [%{reason: :no_shared_dependencies}]}}
  end

  defp shared_enough_dependencies?(deps) do
    boundaries =
      deps
      |> Enum.reject(&(&1.subscriber_count > 1))
      |> Enum.map(fn dep ->
        %{
          node_id: dep.local_node_id,
          graph_node_id: dep.graph_node_id,
          reason: :single_subscriber_dependency,
          subscriber_count: dep.subscriber_count
        }
      end)

    case boundaries do
      [] -> :ok
      _ -> {:error, :single_subscriber_dependency, %{boundaries: boundaries}}
    end
  end

  defp register_source_dependencies(deps) do
    Enum.each(deps, fn
      %{source_registration: {source_id, surface, instance}} ->
        :ok = Subscriptions.register(source_id, surface, instance)

      _dep ->
        :ok
    end)
  end

  defp graph_compute_fun(dep_pairs, local_to_graph, fun) do
    fn graph_values ->
      dep_pairs
      |> Map.new(fn {dep, local_node_id} ->
        graph_node_id = Map.fetch!(local_to_graph, local_node_id)
        {dep, Map.fetch!(graph_values, graph_node_id)}
      end)
      |> fun.()
    end
  end

  defp register_shared_derived(context) do
    Subscriptions.register_derived_and_compute(
      context.graph_node_id,
      context.dep_graph_node_ids,
      context.dep_values,
      context.compute,
      context.metadata
    )
  end

  defp compute_initial_value(socket, dep_node_ids, compute) do
    store = State.store(socket)

    dep_node_ids
    |> Map.new(fn id -> {id, Store.fetch!(store, id)} end)
    |> compute.()
  end

  @spec raise_compute_error(term()) :: no_return()
  defp raise_compute_error({%module{} = exception, stack}) when is_list(stack) do
    if function_exported?(module, :exception, 1) do
      reraise exception, stack
    else
      exit({exception, stack})
    end
  end

  defp raise_compute_error(reason), do: exit(reason)
end
