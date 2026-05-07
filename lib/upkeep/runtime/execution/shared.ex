defmodule Upkeep.Runtime.Execution.Shared do
  @moduledoc false

  alias Upkeep.Coordinator.Graph, as: Coordinator
  alias Upkeep.DAG.Graph, as: DAGGraph
  alias Upkeep.DAG.Store
  alias Upkeep.Runtime.ScopeCapture
  alias Upkeep.Runtime.State
  alias Upkeep.Runtime.Subscriptions

  def initial_value(
        socket,
        assign_name,
        dep_node_ids,
        dep_pairs,
        fun,
        compute,
        source_location \\ nil
      ) do
    base_metadata = sharing_metadata(socket, assign_name, dep_node_ids)

    with {:external, fun_identity} <- ScopeCapture.analyze(fun),
         true <- Subscriptions.shared_initial_load?(socket),
         {:ok, graph_dep_ids, local_to_graph} <- graph_dep_ids(socket, dep_node_ids),
         {:ok, sharing_partition, dep_partitions} <-
           Coordinator.shared_partition_info(graph_dep_ids) do
      ensure_graph_source_deps(socket, graph_dep_ids)

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
      :not_external ->
        local_initial_value(socket, dep_node_ids, compute, base_metadata, :local_fun)

      {:captured_scope, fun_identity, scope_capture} = analysis ->
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

        ScopeCapture.apply_policy(analysis, %{
          assign_name: assign_name,
          source_location: source_location
        })

        {compute_initial_value(socket, dep_node_ids, compute), nil, metadata}

      {:captured, fun_identity} ->
        metadata =
          Map.merge(base_metadata, %{
            result: :local,
            reason: :captured_fun,
            fun: fun_identity,
            implicit_scope: ScopeCapture.implicit_scope_metadata(socket, dep_node_ids)
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

  defp ensure_graph_source_deps(socket, graph_dep_ids) do
    watches = State.watches(socket)

    Enum.each(graph_dep_ids, fn
      source_id when is_map_key(watches, source_id) ->
        watch = Map.fetch!(watches, source_id)

        :ok =
          Subscriptions.register(
            source_id,
            watch.interest_keys,
            watch.source,
            watch.params,
            Map.get(watch, :tracked_deps, [])
          )

      _graph_dep_id ->
        :ok
    end)
  end

  defp sharing_metadata(socket, assign_name, dep_node_ids) do
    %{
      assign_name: assign_name,
      view: Map.get(socket, :view),
      dep_node_ids: dep_node_ids
    }
  end

  defp compute_initial_value(socket, dep_node_ids, compute) do
    store = State.store(socket)

    dep_node_ids
    |> Map.new(fn dep_node_id -> {dep_node_id, Store.fetch!(store, dep_node_id)} end)
    |> compute.()
  end
end
