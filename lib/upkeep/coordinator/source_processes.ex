defmodule Upkeep.Coordinator.SourceProcesses do
  @moduledoc false

  alias Upkeep.Coordinator.SourceProcess
  alias Upkeep.InvalidationSurface
  alias Upkeep.Source.Instance

  @registry Upkeep.Coordinator.SourceProcesses.Registry
  @supervisor Upkeep.Coordinator.SourceProcesses.Supervisor
  @task_sup Upkeep.Coordinator.SourceProcesses.TaskSup

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

  def register_source(node_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    register(node_id, surface, {:source, instance})
  end

  def register_source_and_load(node_id, %InvalidationSurface{} = surface, %Instance{} = instance) do
    with {:ok, pid} <- ensure_started(node_id, surface, {:source, instance}),
         :ok <- SourceProcess.update(pid, surface, {:source, instance}) do
      SourceProcess.load(pid)
    end
  end

  def register_loader(node_id, %InvalidationSurface{} = surface, load_fn)
      when is_function(load_fn, 0) do
    register(node_id, surface, {:fun, load_fn})
  end

  def invalidate(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> SourceProcess.invalidate(pid)
      :error -> :ok
    end
  end

  def touch_subscribers(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> SourceProcess.touch_subscribers(pid)
      :error -> :ok
    end
  end

  def release(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> SourceProcess.release(pid)
      :error -> :ok
    end
  end

  def drain_all do
    @registry
    |> Registry.select([{{:"$1", :"$2", :_}, [], [:"$2"]}])
    |> Enum.each(&SourceProcess.drain/1)

    :ok
  end

  def count do
    Registry.count(@registry)
  end

  def unregister(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> SourceProcess.stop(pid)
      :error -> :ok
    end
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

  defp register(node_id, surface, loader) do
    with {:ok, pid} <- ensure_started(node_id, surface, loader) do
      SourceProcess.update(pid, surface, loader)
    end
  end

  defp ensure_started(node_id, surface, loader) do
    case lookup(node_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        start_child(node_id, surface, loader)
    end
  end

  defp start_child(node_id, surface, loader) do
    child_spec = {SourceProcess, node_id: node_id, surface: surface, loader: loader}

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
