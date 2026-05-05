defmodule Upkeep.Runtime.Execution.Shared do
  @moduledoc false

  alias Upkeep.Coordinator.Graph
  alias Upkeep.DAG.Graph, as: DAGGraph
  alias Upkeep.DAG.Store
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Subscriptions

  def initial_value(socket, assign_name, dep_node_ids, dep_pairs, fun, compute) do
    base_metadata = sharing_metadata(socket, assign_name, dep_node_ids)

    with {:ok, fun_identity} <- external_fun_identity(fun),
         true <- Subscriptions.shared_initial_load?(socket),
         {:ok, graph_dep_ids, local_to_graph} <- graph_dep_ids(socket, dep_node_ids),
         {:ok, sharing_partition, dep_partitions} <- Graph.shared_partition_info(graph_dep_ids) do
      dep_values = graph_dep_values(socket, local_to_graph)

      graph_compute = fn graph_node_values ->
        dep_pairs
        |> Map.new(fn {dep, local_node_id} ->
          graph_node_id = Map.fetch!(local_to_graph, local_node_id)
          {dep, Map.fetch!(graph_node_values, graph_node_id)}
        end)
        |> fun.()
      end

      graph_node_id = {
        :derived,
        socket.view,
        assign_name,
        graph_dep_ids,
        fun_identity
      }

      metadata = %{
        assign_name: assign_name,
        view: socket.view,
        fun: fun_identity,
        sharing_partition: sharing_partition,
        dep_partitions: dep_partitions
      }

      case Subscriptions.register_derived_and_compute(
             graph_node_id,
             graph_dep_ids,
             dep_values,
             graph_compute,
             metadata
           ) do
        {:ok, value} ->
          metadata =
            base_metadata
            |> Map.merge(%{
              result: :shared,
              reason: :shareable,
              graph_node_id: graph_node_id,
              graph_dep_node_ids: graph_dep_ids,
              sharing_partition: sharing_partition,
              dep_partitions: dep_partitions,
              fun: fun_identity,
              compute_fn: graph_compute
            })

          {value, graph_node_id, metadata}
      end
    else
      :not_external_fun ->
        local_initial_value(socket, dep_node_ids, compute, base_metadata, :local_fun)

      {:captured_scope_fun, fun_identity, scope_capture} ->
        metadata =
          base_metadata
          |> Map.merge(%{
            result: :local,
            reason: :captured_scope,
            severity: :error,
            fun: fun_identity,
            scope_capture: scope_capture,
            implicit_scope: :current_scope,
            scope_present?: Map.has_key?(socket.assigns, :current_scope)
          })

        case captured_scope_policy() do
          :raise ->
            raise Upkeep.ImplicitScopeError,
                  "Upkeep derive #{inspect(assign_name)} captures #{inspect(scope_capture)}. " <>
                    "Use an external function that receives current_scope from the dependency map " <>
                    "instead of closing over socket/session/current_scope values."

          :telemetry ->
            {compute_initial_value(socket, dep_node_ids, compute), nil, metadata}
        end

      {:captured_fun, fun_identity} ->
        metadata =
          Map.merge(base_metadata, %{
            result: :local,
            reason: :captured_fun,
            fun: fun_identity,
            implicit_scope: implicit_scope_metadata(socket, dep_node_ids)
          })

        {compute_initial_value(socket, dep_node_ids, compute), nil, metadata}

      false ->
        local_initial_value(socket, dep_node_ids, compute, base_metadata, :disconnected_socket)

      {:unshareable_dep, reason} ->
        local_initial_value(socket, dep_node_ids, compute, base_metadata, reason)

      {:error, :cross_partition_dep, dep_partitions} ->
        local_initial_value(socket, dep_node_ids, compute, base_metadata, :cross_partition_dep,
          dep_partitions: dep_partitions
        )

      {:error, :empty_deps, dep_partitions} ->
        local_initial_value(socket, dep_node_ids, compute, base_metadata, :empty_deps,
          dep_partitions: dep_partitions
        )
    end
  rescue
    ArgumentError ->
      {compute_initial_value(socket, dep_node_ids, compute), nil,
       Map.merge(sharing_metadata(socket, assign_name, dep_node_ids), %{
         result: :local,
         reason: :error
       })}
  end

  def sharing_plan(socket, dep_node_ids, metadata)
      when is_list(dep_node_ids) and is_map(metadata) do
    plan =
      socket
      |> State.store()
      |> Store.graph()
      |> DAGGraph.applicable_subgraphs(dep_node_ids, &classify_shareable_node(socket, &1, &2))

    %{
      final_result: Map.get(metadata, :result),
      final_reason: Map.get(metadata, :reason),
      roots: plan.roots,
      candidate_shareable_nodes: plan.selected_node_ids,
      candidate_shareable_subgraphs: Enum.map(plan.subgraphs, & &1.node_ids),
      largest_shareable_subgraphs: Enum.map(plan.largest_subgraphs, & &1.node_ids),
      boundaries: plan.boundaries
    }
  end

  defp local_initial_value(socket, dep_node_ids, compute, metadata, reason, extra \\ []) do
    metadata =
      metadata
      |> Map.merge(%{result: :local, reason: reason})
      |> Map.merge(Map.new(extra))

    {compute_initial_value(socket, dep_node_ids, compute), nil, metadata}
  end

  defp graph_dep_ids(socket, dep_node_ids) do
    dep_node_ids
    |> Enum.reduce_while({:ok, [], %{}}, fn
      {:source, {:scoped, _component, _source_id}}, _acc ->
        {:halt, {:unshareable_dep, :component_scoped_dep}}

      {:source, source_id} = local_node_id, {:ok, ids, local_to_graph} ->
        {:cont, {:ok, [source_id | ids], Map.put(local_to_graph, local_node_id, source_id)}}

      {:derived, _assign_name} = local_node_id, {:ok, ids, local_to_graph} ->
        case Map.fetch(State.shared_derived_nodes(socket), local_node_id) do
          {:ok, graph_node_id} ->
            {:cont,
             {:ok, [graph_node_id | ids], Map.put(local_to_graph, local_node_id, graph_node_id)}}

          :error ->
            {:halt, {:unshareable_dep, :local_only_dep}}
        end

      _node_id, _acc ->
        {:halt, {:unshareable_dep, :unsupported_dep}}
    end)
    |> case do
      {:ok, ids, local_to_graph} -> {:ok, Enum.reverse(ids), local_to_graph}
      {:unshareable_dep, reason} -> {:unshareable_dep, reason}
    end
  end

  defp classify_shareable_node(_socket, {:source, {:scoped, _component, _source_id}}, _node) do
    {:exclude, :component_scoped_dep}
  end

  defp classify_shareable_node(_socket, {:source, _source_id}, _node), do: :include

  defp classify_shareable_node(socket, {:derived, _assign_name} = local_node_id, _node) do
    case Map.fetch(State.shared_derived_nodes(socket), local_node_id) do
      {:ok, _graph_node_id} -> :include
      :error -> {:exclude, :local_only_dep}
    end
  end

  defp classify_shareable_node(_socket, {:scope, :current_scope}, _node) do
    {:exclude, :current_scope}
  end

  defp classify_shareable_node(_socket, {:component, _component_id}, _node) do
    {:exclude, :component_boundary}
  end

  defp classify_shareable_node(_socket, {:component_assign, _component_id, _assign_name}, _node) do
    {:exclude, :component_boundary}
  end

  defp classify_shareable_node(_socket, _node_id, _node), do: {:exclude, :unsupported_dep}

  defp graph_dep_values(socket, local_to_graph) do
    store = State.store(socket)

    Map.new(local_to_graph, fn {local_node_id, graph_node_id} ->
      {graph_node_id, Store.fetch!(store, local_node_id)}
    end)
  end

  defp external_fun_identity(fun) do
    info = :erlang.fun_info(fun)

    with {:env, []} <- List.keyfind(info, :env, 0),
         {:type, :external} <- List.keyfind(info, :type, 0),
         {:module, module} <- List.keyfind(info, :module, 0),
         {:name, name} <- List.keyfind(info, :name, 0),
         {:arity, arity} <- List.keyfind(info, :arity, 0) do
      {:ok, {module, name, arity}}
    else
      {:env, env} when is_list(env) and env != [] ->
        case scope_like_capture(env) do
          nil -> {:captured_fun, fun_identity_from_info(info)}
          scope_capture -> {:captured_scope_fun, fun_identity_from_info(info), scope_capture}
        end

      _ ->
        :not_external_fun
    end
  end

  defp fun_identity_from_info(info) do
    module = info |> List.keyfind(:module, 0) |> elem(1)
    name = info |> List.keyfind(:name, 0) |> elem(1)
    arity = info |> List.keyfind(:arity, 0) |> elem(1)
    {module, name, arity}
  end

  defp sharing_metadata(socket, assign_name, dep_node_ids) do
    %{
      assign_name: assign_name,
      view: Map.get(socket, :view),
      dep_node_ids: dep_node_ids
    }
  end

  defp implicit_scope_metadata(socket, dep_node_ids) do
    cond do
      not Map.has_key?(socket.assigns, :current_scope) ->
        :missing

      Upkeep.Live.Ids.scope_node_id(:current_scope) in dep_node_ids ->
        :dependency

      true ->
        :available
    end
  end

  defp captured_scope_policy do
    Application.get_env(:upkeep, :captured_scope_policy) || default_captured_scope_policy()
  end

  defp default_captured_scope_policy do
    case runtime_env() do
      :dev -> :raise
      :prod -> :telemetry
      _other -> :telemetry
    end
  end

  defp runtime_env do
    if Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) do
      Mix.env()
    else
      :prod
    end
  end

  defp scope_like_capture(values) do
    Enum.find_value(values, &scope_like_value/1)
  end

  defp scope_like_value(%Phoenix.LiveView.Socket{}), do: :socket
  defp scope_like_value(%{assigns: %{current_scope: _}}), do: :socket
  defp scope_like_value(%{current_scope: _}), do: :current_scope
  defp scope_like_value(%{current_user: _}), do: :current_user
  defp scope_like_value(%{"current_scope" => _}), do: :current_scope
  defp scope_like_value(%{"current_user" => _}), do: :current_user

  defp scope_like_value(map) when is_map(map) do
    map
    |> Map.values()
    |> scope_like_capture()
  end

  defp scope_like_value(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> scope_like_capture()
  end

  defp scope_like_value(list) when is_list(list), do: scope_like_capture(list)
  defp scope_like_value(_value), do: nil

  defp compute_initial_value(socket, dep_node_ids, compute) do
    store = State.store(socket)

    dep_node_ids
    |> Map.new(fn dep_node_id -> {dep_node_id, Store.fetch!(store, dep_node_id)} end)
    |> compute.()
  end
end
