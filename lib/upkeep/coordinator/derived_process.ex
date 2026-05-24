defmodule Upkeep.Coordinator.DerivedProcess do
  @moduledoc false

  use GenServer

  alias Upkeep.Coordinator.DerivedProcesses
  alias Upkeep.Coordinator.Subscriptions

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
    GenServer.start_link(__MODULE__, opts, name: DerivedProcesses.via(node_id))
  end

  def register_and_compute(pid, dep_node_ids, dep_values, compute, metadata) do
    GenServer.call(
      pid,
      {:register_and_compute, dep_node_ids, dep_values, compute, metadata},
      60_000
    )
  end

  def release(pid), do: safe_call(pid, :release, 60_000)
  def drain(pid), do: safe_call(pid, :drain, 60_000)

  @impl true
  def init(opts) do
    node_id = Keyword.fetch!(opts, :node_id)

    {:ok,
     %{
       node_id: node_id,
       encoded_key: Subscriptions.source_key(node_id),
       dep_node_ids: [],
       dep_values: %{},
       compute: nil,
       metadata: %{},
       value: nil,
       loaded?: false,
       loading: nil,
       dirty?: false,
       waiters: [],
       drain_waiters: []
     }}
  end

  @impl true
  def handle_call(
        {:register_and_compute, dep_node_ids, dep_values, compute, metadata},
        from,
        state
      ) do
    {state, deps_changed?} =
      update_definition(state, dep_node_ids, dep_values, compute, metadata)

    state =
      if deps_changed? and not is_nil(state.loading) do
        %{state | dirty?: true}
      else
        state
      end

    cond do
      state.loaded? and not deps_changed? ->
        GenServer.reply(from, {:ok, state.value})
        {:noreply, state}

      state.loaded? and not is_nil(state.loading) ->
        GenServer.reply(from, {:ok, state.value})
        {:noreply, state}

      state.loaded? ->
        state =
          state
          |> add_waiter(from)
          |> start_compute(:refresh, true)

        {:noreply, state}

      true ->
        state =
          state
          |> add_waiter(from)
          |> start_compute(:initial_load, false)

        {:noreply, state}
    end
  end

  @impl true
  def handle_call(:release, _from, state) do
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_call(:drain, _from, %{loading: nil} = state), do: {:reply, :ok, state}

  @impl true
  def handle_call(:drain, from, state) do
    {:noreply, %{state | drain_waiters: [from | state.drain_waiters]}}
  end

  @impl true
  def handle_info({:dag_values, pairs}, state) when is_list(pairs) do
    state =
      case merge_dep_values(state, pairs) do
        {:changed, state} -> start_or_mark_dirty(state, :refresh)
        :unchanged -> state
      end

    {:noreply, state}
  end

  @impl true
  def handle_info({ref, value}, %{loading: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    dispatch? = state.loading.dispatch?

    state =
      state
      |> finish_compute()
      |> handle_computed(value, dispatch?)
      |> reply_drains_if_idle()

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{loading: %{ref: ref}} = state) do
    state =
      state
      |> finish_compute()
      |> reply_waiters({:error, reason})
      |> Map.put(:dirty?, false)
      |> reply_drains_if_idle()

    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.dep_node_ids, &Subscriptions.unsubscribe/1)
    :ok
  end

  defp update_definition(state, dep_node_ids, dep_values, compute, metadata) do
    old_dep_values = state.dep_values

    state =
      state
      |> reconcile_dep_subscriptions(dep_node_ids)
      |> Map.merge(%{
        dep_node_ids: dep_node_ids,
        dep_values: dep_values,
        compute: compute,
        metadata: metadata
      })

    {state, old_dep_values != dep_values}
  end

  defp reconcile_dep_subscriptions(state, dep_node_ids) do
    old = MapSet.new(state.dep_node_ids)
    new = MapSet.new(dep_node_ids)

    new
    |> MapSet.difference(old)
    |> Enum.each(&Subscriptions.subscribe(&1, %{kind: :derived_process, node_id: state.node_id}))

    old
    |> MapSet.difference(new)
    |> Enum.each(&Subscriptions.unsubscribe/1)

    state
  end

  defp merge_dep_values(state, pairs) do
    dep_node_ids = MapSet.new(state.dep_node_ids)

    pushed =
      pairs
      |> Enum.filter(fn {node_id, _value} -> MapSet.member?(dep_node_ids, node_id) end)
      |> Map.new()

    dep_values = Map.merge(state.dep_values, pushed)

    if pushed == %{} or dep_values == state.dep_values do
      :unchanged
    else
      {:changed, %{state | dep_values: dep_values}}
    end
  end

  defp start_or_mark_dirty(%{loading: nil} = state, reason) do
    start_compute(state, reason, true)
  end

  defp start_or_mark_dirty(state, _reason), do: %{state | dirty?: true}

  defp start_compute(%{loading: nil, compute: compute} = state, reason, dispatch?)
       when is_function(compute, 1) do
    dep_values = state.dep_values
    metadata = compute_metadata(state, reason)

    task =
      Task.Supervisor.async_nolink(DerivedProcesses.task_sup(), fn ->
        :telemetry.span([:upkeep, :graph, :derived_compute], metadata, fn ->
          value = compute.(dep_values)
          {value, Map.put(metadata, :dep_count, map_size(dep_values))}
        end)
      end)

    %{state | loading: %{ref: task.ref, reason: reason, dispatch?: dispatch?}}
  end

  defp start_compute(state, _reason, _dispatch?), do: state

  defp handle_computed(%{dirty?: true} = state, _value, _dispatch?) do
    state
    |> Map.put(:dirty?, false)
    |> start_compute(:refresh, true)
  end

  defp handle_computed(state, value, dispatch?) do
    changed? = not state.loaded? or state.value != value
    state = %{state | value: value, loaded?: true}

    if dispatch? and changed? do
      dispatch(state, [{state.node_id, value}])
    end

    reply_waiters(state, {:ok, value})
  end

  defp dispatch(state, pairs) do
    metadata = %{
      backend: :derived_process,
      node_id: state.node_id,
      pair_count: length(pairs),
      dep_count: length(state.dep_node_ids)
    }

    :telemetry.span([:upkeep, :graph, :dispatch], metadata, fn ->
      pids =
        state.encoded_key
        |> Subscriptions.members()
        |> Enum.map(fn {pid, _meta} -> pid end)
        |> Enum.uniq()

      Enum.each(pids, fn pid ->
        send(pid, {:dag_values, pairs})
      end)

      {:ok, Map.put(metadata, :pid_count, length(pids))}
    end)
  end

  defp compute_metadata(state, reason) do
    state.metadata
    |> Map.put(:backend, :derived_process)
    |> Map.put(:node_id, state.node_id)
    |> Map.put(:compute_reason, reason)
    |> Map.put(:subscriber_count, Subscriptions.member_count(state.encoded_key))
  end

  defp finish_compute(state), do: %{state | loading: nil}

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

  defp safe_call(pid, message, timeout) do
    GenServer.call(pid, message, timeout)
  catch
    :exit, {:normal, _call} -> :ok
    :exit, {:noproc, _call} -> :ok
  end
end
