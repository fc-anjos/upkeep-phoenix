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
    register_source_and_load(node_id, surface, instance, _attempts = 1)
  end

  defp register_source_and_load(node_id, surface, instance, attempts) do
    loader = {:source, instance}

    with {:ok, pid} <- ensure_started(node_id, surface, loader),
         :ok <- safe_update(pid, surface, loader) do
      SourceProcess.load(pid)
    else
      :dead when attempts > 0 ->
        # The looked-up source process died between lookup and update (e.g. idle
        # timeout / release racing registration). Drop the stale registration and
        # retry once with a fresh process instead of crashing the caller.
        forget(node_id)
        register_source_and_load(node_id, surface, instance, attempts - 1)

      other ->
        other
    end
  end

  def register_loader(node_id, %InvalidationSurface{} = surface, load_fn)
      when is_function(load_fn, 0) do
    register(node_id, surface, {:fun, load_fn})
  end

  @doc """
  Deliver an invalidation without blocking the caller.

  Used on the steady-state notification path so that a single slow or wedged
  source process cannot stall invalidation delivery to every other source.
  """
  def invalidate_async(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> SourceProcess.invalidate_async(pid)
      :error -> :ok
    end
  end

  def invalidate(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> SourceProcess.invalidate(pid)
      :error -> :ok
    end
  end

  @doc """
  Synchronization barrier for a single source process.

  Confirms that any earlier `invalidate_async/1` cast from the same caller has
  been applied, without re-marking the source. Used by the drain path.
  """
  def sync(node_id) do
    case lookup(node_id) do
      {:ok, pid} -> SourceProcess.sync(pid)
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

  defp register(node_id, surface, loader), do: register(node_id, surface, loader, _attempts = 1)

  defp register(node_id, surface, loader, attempts) do
    with {:ok, pid} <- ensure_started(node_id, surface, loader),
         :ok <- safe_update(pid, surface, loader) do
      :ok
    else
      :dead when attempts > 0 ->
        # The looked-up source process died between lookup and update; drop the
        # stale registration and retry once rather than crashing the caller.
        forget(node_id)
        register(node_id, surface, loader, attempts - 1)

      other ->
        other
    end
  end

  defp ensure_started(node_id, surface, loader) do
    case lookup(node_id) do
      {:ok, pid} ->
        if Process.alive?(pid) do
          {:ok, pid}
        else
          # Stale Registry entry for a just-died process (the Registry's own DOWN
          # cleanup may not have run yet). Start a fresh process; if the entry is
          # still claimed, `start_child` returns whatever is registered now.
          start_child(node_id, surface, loader)
        end

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

  # Update a source process, degrading a dead-process exit (the looked-up pid
  # died between lookup and this call) to `:dead` so the caller can retry with a
  # fresh process instead of crashing.
  defp safe_update(pid, surface, loader) do
    SourceProcess.update(pid, surface, loader)
  catch
    :exit, {:noproc, _call} -> :dead
    :exit, {:normal, _call} -> :dead
    :exit, {:shutdown, _call} -> :dead
  end

  # Wait briefly for the Registry to drop a dead source process's entry. The
  # Registry removes entries via its own monitor when the process exits, but that
  # cleanup is asynchronous; this bounded wait lets the subsequent retry start a
  # fresh process instead of re-finding the just-died one.
  defp forget(node_id), do: forget(node_id, _deadline = System.monotonic_time(:millisecond) + 200)

  defp forget(node_id, deadline) do
    case lookup(node_id) do
      {:ok, pid} ->
        cond do
          Process.alive?(pid) -> :ok
          System.monotonic_time(:millisecond) >= deadline -> :ok
          true -> forget(node_id, deadline)
        end

      :error ->
        :ok
    end
  end

  defp lookup(node_id) do
    case Registry.lookup(@registry, node_id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end
end
