defmodule Upkeep.Coordinator.DerivedProcesses do
  @moduledoc false

  alias Upkeep.Coordinator.DerivedProcess

  @registry Upkeep.Coordinator.DerivedProcesses.Registry
  @supervisor Upkeep.Coordinator.DerivedProcesses.Supervisor
  @task_sup Upkeep.Coordinator.DerivedProcesses.TaskSup

  def registry, do: @registry
  def supervisor, do: @supervisor
  def task_sup, do: @task_sup

  def child_specs do
    [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @supervisor},
      {Task.Supervisor, name: @task_sup}
    ]
  end

  def via(node_id), do: {:via, Registry, {@registry, node_id}}

  def register_and_compute(node_id, dep_node_ids, dep_values, compute, metadata)
      when is_list(dep_node_ids) and is_map(dep_values) and is_function(compute, 1) do
    with {:ok, pid} <- ensure_started(node_id) do
      DerivedProcess.register_and_compute(pid, dep_node_ids, dep_values, compute, metadata)
    end
  end

  def release(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> DerivedProcess.release(pid)
      :error -> :ok
    end
  end

  def drain_all do
    @registry
    |> Registry.select([{{:"$1", :"$2", :_}, [], [:"$2"]}])
    |> Enum.each(&DerivedProcess.drain/1)

    :ok
  end

  def count do
    Registry.count(@registry)
  end

  def reset_all do
    if Process.whereis(@supervisor) do
      @supervisor
      |> DynamicSupervisor.which_children()
      |> Enum.each(fn {_id, pid, _type, _modules} ->
        DynamicSupervisor.terminate_child(@supervisor, pid)
      end)
    end

    :ok
  end

  defp ensure_started(node_id) do
    case lookup(node_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        start_child(node_id)
    end
  end

  defp start_child(node_id) do
    child_spec = {DerivedProcess, node_id: node_id}

    case DynamicSupervisor.start_child(@supervisor, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, {:already_registered, pid}} -> {:ok, pid}
      other -> other
    end
  end

  defp lookup(node_id) do
    case Registry.lookup(@registry, node_id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end
end
