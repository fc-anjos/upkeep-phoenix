defmodule Upkeep.Coordinator.Graph.Shard do
  @moduledoc false
  use GenServer

  alias Upkeep.Coordinator.Graph.Shard.{Flush, InitialLoads, Lifecycle, Nodes, Retries}
  alias Upkeep.DAG.Store
  alias Upkeep.Coordinator.DirtyBuffer
  alias Upkeep.Coordinator.Retry
  alias Upkeep.SingleFlight

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
       source_loads: SingleFlight.new(),
       derived_loads: SingleFlight.new(),
       retries: Retry.new(retry_opts()),
       buffer: DirtyBuffer.new(threshold: 1_000)
     }}
  end

  @impl true
  def handle_call({:register_source, node_id, surface, loader}, _from, state) do
    {:reply, :ok, Nodes.register_source(state, node_id, surface, loader)}
  end

  @impl true
  def handle_call({:register_source_and_load, node_id, surface, loader}, from, state) do
    state = Nodes.register_source(state, node_id, surface, loader)
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
        source_loads: SingleFlight.new(),
        derived_loads: SingleFlight.new(),
        retries: Retry.new(retry_opts()),
        buffer: DirtyBuffer.new(threshold: 1_000)
      })

    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:notify_source_nodes, node_ids}, state) do
    state =
      case node_ids do
        [] -> state
        node_ids -> Flush.enqueue(state, node_ids)
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state), do: {:noreply, Flush.flush(state)}

  @impl true
  def handle_info({:upkeep_invalidation, _origin, _event}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:upkeep_invalidation, _event}, state), do: {:noreply, state}

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
  def handle_info({ref, %Upkeep.Coordinator.LoadedSource{} = loaded}, state) do
    {:noreply, InitialLoads.handle_source_result(state, ref, loaded)}
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
    SingleFlight.demonitor_all(state.source_loads)
    SingleFlight.demonitor_all(state.derived_loads)
    state
  end

  defp retry_opts do
    Application.get_env(:upkeep, :graph_retry, [])
  end
end
