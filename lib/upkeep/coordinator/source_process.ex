defmodule Upkeep.Coordinator.SourceProcess do
  @moduledoc false

  use GenServer

  alias Upkeep.Coordinator.{LoadedSource, LoadFailure, Node, Retry, SourceProcesses}
  alias Upkeep.Coordinator.SourceLoader
  alias Upkeep.Coordinator.{Subscriptions, Topology}
  alias Upkeep.InvalidationSurface

  def child_spec(opts) do
    node_id = Keyword.fetch!(opts, :node_id)

    %{
      id: {__MODULE__, node_id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  def start_link(opts) do
    node_id = Keyword.fetch!(opts, :node_id)
    GenServer.start_link(__MODULE__, opts, name: SourceProcesses.via(node_id))
  end

  def update(pid, %InvalidationSurface{} = surface, loader) do
    GenServer.call(pid, {:update, surface, loader})
  end

  def load(pid) do
    GenServer.call(pid, :load, load_timeout())
  catch
    # A slow or unresponsive source must not exit the calling LiveView. Degrade
    # a timeout/exit to an error result the caller can handle (mount/refresh
    # turn it into an error assign instead of crashing).
    :exit, {:timeout, _call} -> {:error, :timeout}
    :exit, {:normal, _call} -> {:error, :noproc}
    :exit, {:noproc, _call} -> {:error, :noproc}
  end
  # Non-blocking invalidation delivery. The handler only marks the source dirty
  # / triggers a reload and needs no return value, so a wedged or slow source
  # cannot stall the caller (e.g. the single Notifier funnel) — see invalidate/1
  # for the blocking variant used as a drain barrier in tests.
  def invalidate_async(pid), do: GenServer.cast(pid, :invalidate)
  def invalidate(pid), do: safe_call(pid, :invalidate, 60_000)

  # A pure synchronization barrier: a no-op call whose only purpose is to confirm
  # that any earlier `invalidate_async/1` cast from the same sender has been
  # processed (per-pair FIFO ordering). It does NOT re-mark the source, so using
  # it as a drain barrier never triggers an extra reload.
  def sync(pid), do: safe_call(pid, :sync, 60_000)
  def touch_subscribers(pid), do: safe_call(pid, :touch_subscribers)
  def release(pid), do: safe_call(pid, :release, 60_000)
  def drain(pid), do: safe_call(pid, :drain, 60_000)
  def stop(pid), do: safe_call(pid, :stop, 60_000)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    node_id = Keyword.fetch!(opts, :node_id)
    surface = Keyword.fetch!(opts, :surface)
    loader = Keyword.fetch!(opts, :loader)
    partition = Topology.node_partition(node_id)
    node = node(node_id, surface, loader)

    Topology.register_source(node_id, surface)

    {:ok,
     %{
       partition: partition,
       node_id: node_id,
       node: node,
       value: nil,
       cached_reply: nil,
       loading: nil,
       waiters: [],
       drain_waiters: [],
       dirty?: false,
       stale?: false,
       idle?: false,
       idle_timer_ref: nil,
       idle_ttl_ms: idle_ttl_ms(loader),
       max_subscribers: 0,
       retries: Retry.new(retry_opts())
     }}
  end

  @impl true
  def handle_call({:update, surface, loader}, _from, state) do
    node = node(state.node_id, surface, loader, state.node.loaded?)
    state = activate(state)

    if surface != state.node.surface do
      Topology.reconcile_source(state.node_id, surface)
    end

    {:reply, :ok, %{state | node: node, idle_ttl_ms: idle_ttl_ms(loader)}}
  end

  @impl true
  def handle_call(:load, from, state) do
    state = activate(state)

    if state.node.loaded? and not state.stale? do
      GenServer.reply(from, state.cached_reply)
      {:noreply, state}
    else
      state =
        state
        |> add_waiter(from)
        |> start_load(:initial_load)

      {:noreply, state}
    end
  end

  @impl true
  def handle_call(:touch_subscribers, _from, state) do
    subscriber_count = Subscriptions.member_count(state.node.encoded_key)

    state =
      state
      |> activate()
      |> Map.update!(:max_subscribers, &max(&1, subscriber_count))

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:release, _from, state) do
    state = cancel_idle_timer(state)

    if retain_idle?(state) do
      {:reply, :ok, enter_idle(state), :hibernate}
    else
      {:stop, :normal, :ok, state}
    end
  end

  @impl true
  def handle_call(:drain, _from, %{loading: nil} = state), do: {:reply, :ok, state}

  @impl true
  def handle_call(:drain, from, state) do
    {:noreply, %{state | drain_waiters: [from | state.drain_waiters]}}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    Topology.unregister(state.node_id)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_call(:invalidate, _from, state) do
    {:reply, :ok, apply_invalidate(state)}
  end

  @impl true
  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_cast(:invalidate, state) do
    {:noreply, apply_invalidate(state)}
  end

  @impl true
  def handle_info({ref, %LoadedSource{} = loaded}, %{loading: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])

    state =
      state
      |> finish_load()
      |> handle_loaded(loaded)
      |> reply_drains_if_idle()

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{loading: %{ref: ref}} = state) do
    load_reason = load_reason(state)

    state =
      state
      |> finish_load()
      |> handle_load_failure(reason, load_reason)
      |> reply_drains_if_idle()

    {:noreply, state}
  end

  @impl true
  def handle_info({:retry_source, timer_ref}, state) do
    case Retry.pop_timer(state.retries, state.node_id, timer_ref) do
      {:ok, retries} ->
        {:noreply, %{state | retries: retries} |> start_load(:refresh)}

      :stale ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(
        {:idle_timeout, timer_ref},
        %{idle_timer_ref: {timer_ref, _process_ref}} = state
      ) do
    {:stop, :normal, %{state | idle_timer_ref: nil}}
  end

  @impl true
  def handle_info({:idle_timeout, _timer_ref}, state), do: {:noreply, state}

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Topology.unregister(state.node_id)
    :ok
  end

  defp apply_invalidate(state) do
    state = %{state | stale?: true, retries: Retry.clear(state.retries, state.node_id)}

    if state.idle? do
      state
    else
      start_or_mark_dirty(state, :refresh)
    end
  end

  defp start_or_mark_dirty(%{loading: nil} = state, reason), do: start_load(state, reason)
  defp start_or_mark_dirty(state, _reason), do: %{state | dirty?: true}

  defp start_load(%{loading: nil} = state, reason) do
    generation = Topology.generation(state.node_id)

    metadata =
      state
      |> load_metadata(reason)
      |> Map.put(:source_generation, generation)

    task =
      Task.Supervisor.async_nolink(SourceProcesses.task_sup(), fn ->
        state.node_id
        |> SourceLoader.run_with_deps(state.node, metadata)
        |> LoadedSource.with_generation(generation)
      end)

    %{state | loading: %{ref: task.ref, reason: reason, generation: generation}}
  end

  defp start_load(state, _reason), do: state

  defp handle_loaded(%{dirty?: true} = state, _loaded) do
    %{state | dirty?: false}
    |> start_load(:refresh)
  end

  defp handle_loaded(state, %LoadedSource{} = loaded) do
    if current_generation?(state, loaded) do
      state
      |> apply_loaded(loaded)
      |> reply_waiters(LoadedSource.reply(loaded))
    else
      start_load(state, :refresh)
    end
  end

  defp handle_load_failure(state, reason, load_reason) do
    {retries, retry_metadata} = retry_after_failure(state)

    state = %{state | retries: retries}

    failure = LoadFailure.new(state.node_id, state.node, reason, load_reason, retry_metadata)
    LoadFailure.emit(state, failure)

    reply_waiters(state, {:error, reason})
  end

  defp retry_after_failure(%{node: %{loaded?: true}} = state) do
    Retry.after_failure(state.retries, state.node_id, state.node.retry, &schedule_retry/3)
  end

  defp retry_after_failure(state) do
    {state.retries, Retry.no_retry_metadata(state.retries)}
  end

  defp schedule_retry(_node_id, timer_ref, delay_ms) do
    Process.send_after(self(), {:retry_source, timer_ref}, delay_ms)
  end

  defp apply_loaded(state, %LoadedSource{} = loaded) do
    if loaded.surface != state.node.surface do
      Topology.reconcile_source(state.node_id, loaded.surface)
    end

    %Node{} = current_node = state.node

    node = %{
      current_node
      | surface_keys: loaded.surface_keys,
        surface: loaded.surface,
        tracked_deps: loaded.tracked_deps,
        loaded?: true
    }

    state = %{
      state
      | node: node,
        value: loaded.value,
        cached_reply: LoadedSource.reply(loaded),
        stale?: false,
        retries: Retry.clear(state.retries, state.node_id)
    }

    dispatch(state, [{state.node_id, loaded.value}])
    state
  end

  defp dispatch(state, pairs) do
    metadata = %{
      backend: :source_process,
      partition: state.partition,
      pair_count: length(pairs),
      node_partitions:
        Enum.map(pairs, fn {node_id, _value} -> {node_id, Topology.node_partition(node_id)} end)
    }

    :telemetry.span([:upkeep, :graph, :dispatch], metadata, fn ->
      pids =
        state.node.encoded_key
        |> Subscriptions.members()
        |> Enum.map(fn {pid, _meta} -> pid end)
        |> Enum.uniq()

      Enum.each(pids, fn pid ->
        send(pid, {:dag_values, pairs})
      end)

      {:ok, Map.put(metadata, :pid_count, length(pids))}
    end)
  end

  defp node(node_id, %InvalidationSurface{} = surface, loader, loaded? \\ false) do
    %Node{
      loader: loader,
      encoded_key: Subscriptions.source_key(node_id),
      surface_keys: InvalidationSurface.index_keys(surface),
      surface: surface,
      loaded?: loaded?,
      retry: SourceLoader.retry_config(loader)
    }
  end

  defp load_metadata(state, reason) do
    state.node.loader
    |> SourceLoader.metadata()
    |> Map.put(:backend, :source_process)
    |> Map.put(:partition, state.partition)
    |> Map.put(:node_id, state.node_id)
    |> Map.put(:load_reason, reason)
    |> Map.put(:subscriber_count, Subscriptions.member_count(state.node.encoded_key))
  end

  defp load_reason(%{loading: %{reason: reason}}), do: reason
  defp load_reason(_state), do: :refresh

  defp finish_load(state), do: %{state | loading: nil}

  defp current_generation?(state, %LoadedSource{generation: generation}) do
    Topology.generation(state.node_id) == generation
  end

  defp add_waiter(state, from), do: %{state | waiters: [from | state.waiters]}

  defp reply_waiters(state, reply) do
    Enum.each(state.waiters, &GenServer.reply(&1, reply))
    %{state | waiters: []}
  end

  defp reply_drains_if_idle(%{loading: nil} = state) do
    Enum.each(state.drain_waiters, &GenServer.reply(&1, :ok))
    %{state | drain_waiters: []}
  end

  defp reply_drains_if_idle(state), do: state

  defp retry_opts do
    Application.get_env(:upkeep, :graph_retry, [])
  end

  defp load_timeout do
    Application.get_env(:upkeep, :source_load_timeout_ms, 30_000)
  end

  defp safe_call(pid, message, timeout \\ 5_000) do
    GenServer.call(pid, message, timeout)
  catch
    :exit, {:normal, _call} -> :ok
    :exit, {:noproc, _call} -> :ok
  end

  defp retain_idle?(state) do
    state.max_subscribers > 1 and state.node.loaded? and is_nil(state.loading) and
      state.idle_ttl_ms != 0
  end

  defp enter_idle(state) do
    state
    |> cancel_idle_timer()
    |> Map.put(:idle?, true)
    |> schedule_idle_timer()
    |> cancel_retries()
  end

  defp activate(state) do
    state
    |> cancel_idle_timer()
    |> Map.put(:idle?, false)
  end

  defp schedule_idle_timer(%{idle_ttl_ms: :infinity} = state), do: state

  defp schedule_idle_timer(%{idle_ttl_ms: ttl_ms} = state)
       when is_integer(ttl_ms) and ttl_ms > 0 do
    timer_ref = make_ref()
    process_ref = Process.send_after(self(), {:idle_timeout, timer_ref}, ttl_ms)
    %{state | idle_timer_ref: {timer_ref, process_ref}}
  end

  defp schedule_idle_timer(state), do: state

  defp cancel_idle_timer(%{idle_timer_ref: nil} = state), do: state

  defp cancel_idle_timer(%{idle_timer_ref: {_timer_ref, process_ref}} = state) do
    Process.cancel_timer(process_ref)
    %{state | idle_timer_ref: nil}
  end

  defp cancel_retries(state) do
    %{state | retries: Retry.cancel_all(state.retries)}
  end

  defp idle_ttl_ms({:source, %{idle_ttl_ms: ttl_ms}}), do: normalize_idle_ttl_ms(ttl_ms)
  defp idle_ttl_ms(_loader), do: normalize_idle_ttl_ms(nil)

  defp normalize_idle_ttl_ms(nil) do
    Application.get_env(:upkeep, :source_idle_ttl_ms, 30_000)
    |> normalize_idle_ttl_ms()
  end

  defp normalize_idle_ttl_ms(:infinity), do: :infinity

  defp normalize_idle_ttl_ms(ttl_ms) when is_integer(ttl_ms) and ttl_ms >= 0 do
    ttl_ms
  end

  defp normalize_idle_ttl_ms(ttl_ms) do
    raise ArgumentError,
          "expected source idle TTL to be nil, :infinity, or a non-negative integer, got: #{inspect(ttl_ms)}"
  end
end
