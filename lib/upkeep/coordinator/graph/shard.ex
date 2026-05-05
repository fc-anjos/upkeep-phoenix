defmodule Upkeep.Coordinator.Graph.Shard do
  @moduledoc false
  use GenServer

  alias Upkeep.Coordinator.Graph.Shard.{Flush, InitialLoads, Lifecycle, Nodes, Retries}
  alias Upkeep.DAG.Store

  ## Public

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    idx = Keyword.fetch!(opts, :idx)
    GenServer.start_link(__MODULE__, idx, name: name)
  end

  ## GenServer

  @impl true
  def init(idx) do
    generation = Lifecycle.start(idx)

    {:ok,
     %{
       idx: idx,
       generation: generation,
       store: Store.new(),
       initial_loads: %{},
       initial_load_refs: %{},
       initial_derived_loads: %{},
       initial_derived_load_refs: %{},
       retry_attempts: %{},
       retry_timers: %{},
       buffer_node_ids: MapSet.new(),
       buffer_size: 0,
       flush_scheduled?: false
     }}
  end

  @impl true
  def handle_call({:register_source, node_id, interest_keys, loader}, _from, state) do
    {:reply, :ok, Nodes.register_source(state, node_id, interest_keys, loader)}
  end

  @impl true
  def handle_call({:register_source_and_load, node_id, interest_keys, loader}, from, state) do
    state = Nodes.register_source(state, node_id, interest_keys, loader)
    InitialLoads.register_source_and_load(state, node_id, from)
  end

  @impl true
  def handle_call({:register_derived, node_id, dep_ids, compute_fn}, _from, state) do
    {:reply, :ok, Nodes.register_derived(state, node_id, dep_ids, compute_fn)}
  end

  @impl true
  def handle_call(
        {:register_derived_and_compute, node_id, dep_ids, dep_values, compute_fn, metadata},
        from,
        state
      ) do
    InitialLoads.register_derived_and_compute(
      state,
      node_id,
      dep_ids,
      dep_values,
      compute_fn,
      metadata,
      from
    )
  end

  @impl true
  def handle_call(:drain, _from, state), do: {:reply, :ok, Flush.flush(state)}

  @impl true
  def handle_call(:reset, _from, state) do
    state =
      state
      |> demonitor_initial_loads()
      |> Retries.cancel_all()
      |> Map.merge(%{
        store: Store.new(),
        initial_loads: %{},
        initial_load_refs: %{},
        initial_derived_loads: %{},
        initial_derived_load_refs: %{},
        retry_attempts: %{},
        retry_timers: %{},
        buffer_node_ids: MapSet.new(),
        buffer_size: 0,
        flush_scheduled?: false
      })

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:flush, state), do: {:noreply, Flush.flush(state)}

  @impl true
  def handle_info({:upkeep_graph_notify, event}, state) do
    node_ids = Upkeep.Coordinator.Graph.affected_source_node_ids(event, state.idx)

    state =
      case node_ids do
        [] -> state
        node_ids -> Flush.enqueue(state, node_ids)
      end

    {:noreply, state}
  end

  @impl true
  def handle_info({:retry_source, node_id, timer_ref}, state) do
    case Retries.pop_timer(state, node_id, timer_ref) do
      {:ok, state} ->
        state =
          if Store.has_node?(state.store, node_id) do
            Flush.enqueue_retry(state, [node_id])
          else
            state
          end

        {:noreply, state}

      :stale ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(
        {ref, {node_id, value, current_keys, tracked_deps, node}},
        state
      ) do
    {:noreply,
     InitialLoads.handle_source_result(
       state,
       ref,
       node_id,
       value,
       current_keys,
       tracked_deps,
       node
     )}
  end

  @impl true
  def handle_info({ref, {node_id, value}}, state) do
    {:noreply, InitialLoads.handle_derived_result(state, ref, node_id, value)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {:noreply, InitialLoads.handle_down(state, ref, reason)}
  end

  @impl true
  def handle_info({:group, events, _info}, state) do
    {:noreply, Lifecycle.handle_group_events(events, state)}
  end

  defp demonitor_initial_loads(state) do
    state.initial_load_refs
    |> Map.keys()
    |> Enum.concat(Map.keys(state.initial_derived_load_refs))
    |> Enum.each(&Process.demonitor(&1, [:flush]))

    state
  end
end
