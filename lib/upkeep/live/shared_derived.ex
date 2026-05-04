defmodule Upkeep.Live.SharedDerived do
  @moduledoc false

  alias Upkeep.Live.{State, Subscriptions}
  alias Upkeep.Coordinator.Graph

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
        {compute_initial_value(socket, dep_node_ids, compute), nil,
         Map.merge(base_metadata, %{result: :local, reason: :local_fun})}

      {:captured_fun, fun_identity} ->
        {compute_initial_value(socket, dep_node_ids, compute), nil,
         Map.merge(base_metadata, %{result: :local, reason: :captured_fun, fun: fun_identity})}

      false ->
        {compute_initial_value(socket, dep_node_ids, compute), nil,
         Map.merge(base_metadata, %{result: :local, reason: :disconnected_socket})}

      {:unshareable_dep, reason} ->
        {compute_initial_value(socket, dep_node_ids, compute), nil,
         Map.merge(base_metadata, %{result: :local, reason: reason})}

      {:error, :cross_partition_dep, dep_partitions} ->
        {compute_initial_value(socket, dep_node_ids, compute), nil,
         Map.merge(base_metadata, %{
           result: :local,
           reason: :cross_partition_dep,
           dep_partitions: dep_partitions
         })}

      {:error, :empty_deps, dep_partitions} ->
        {compute_initial_value(socket, dep_node_ids, compute), nil,
         Map.merge(base_metadata, %{
           result: :local,
           reason: :empty_deps,
           dep_partitions: dep_partitions
         })}
    end
  rescue
    ArgumentError ->
      {compute_initial_value(socket, dep_node_ids, compute), nil,
       Map.merge(sharing_metadata(socket, assign_name, dep_node_ids), %{
         result: :local,
         reason: :error
       })}
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

  defp graph_dep_values(socket, local_to_graph) do
    dag = State.dag(socket)

    Map.new(local_to_graph, fn {local_node_id, graph_node_id} ->
      {graph_node_id, Upkeep.DAG.fetch!(dag, local_node_id)}
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
        {:captured_fun, fun_identity_from_info(info)}

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

  defp compute_initial_value(socket, dep_node_ids, compute) do
    socket
    |> State.dag()
    |> then(fn dag ->
      dep_node_ids
      |> Map.new(fn dep_node_id -> {dep_node_id, Upkeep.DAG.fetch!(dag, dep_node_id)} end)
      |> compute.()
    end)
  end
end
