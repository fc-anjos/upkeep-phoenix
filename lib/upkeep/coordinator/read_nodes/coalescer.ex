defmodule Upkeep.Coordinator.ReadNodes.Coalescer do
  @moduledoc """
  Single-flight coordinator for read-node loads.

  When N processes call `Upkeep.read/1` concurrently for the same query
  before a value is cached, only one runs the database load; the rest
  block until the loader settles, then receive the same value. Without
  this, every concurrent cold mount fans out into N duplicate queries.

  The coalescer only owns *metadata* about pending loads; the actual
  `repo.all/1` call runs in the loader's own process so the database
  pool is never single-threaded by this module.

  Loaders are monitored: if a loader crashes or exits before settling,
  every waiter receives `{:error, %Upkeep.Coordinator.ReadNodes.Coalescer.LoaderDown{...}}`
  so callers can retry rather than block forever.
  """

  use GenServer

  defmodule LoaderDown do
    @moduledoc false
    defexception [:node_id, :reason]

    @impl true
    def message(%__MODULE__{node_id: node_id, reason: reason}) do
      "Upkeep read-node loader for #{inspect(node_id)} exited: #{inspect(reason)}"
    end
  end

  @type node_id :: term()

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Run `fun` exactly once across all concurrent callers for `node_id`.

  The caller that wins the reservation runs `fun` and returns its result.
  Concurrent callers block on a tagged message and return the same value.
  """
  @spec coalesce(node_id, (-> term())) :: term()
  def coalesce(node_id, fun) when is_function(fun, 0) do
    case GenServer.call(__MODULE__, {:reserve, node_id}, :infinity) do
      :load ->
        try do
          value = fun.()
          GenServer.cast(__MODULE__, {:settle, node_id, {:ok, value}})
          value
        catch
          kind, reason ->
            stack = __STACKTRACE__
            GenServer.cast(__MODULE__, {:settle, node_id, {:raise, kind, reason, stack}})
            :erlang.raise(kind, reason, stack)
        end

      {:wait, ref} ->
        receive do
          {^ref, {:ok, value}} ->
            value

          {^ref, {:raise, kind, reason, stack}} ->
            :erlang.raise(kind, reason, stack)

          {^ref, {:loader_down, reason}} ->
            raise LoaderDown, node_id: node_id, reason: reason
        end
    end
  end

  @doc false
  def pending?(node_id), do: GenServer.call(__MODULE__, {:pending?, node_id})

  @impl true
  def init(_), do: {:ok, %{pending: %{}, monitors: %{}}}

  @impl true
  def handle_call({:reserve, node_id}, {pid, _tag}, state) do
    case Map.get(state.pending, node_id) do
      nil ->
        mref = Process.monitor(pid)
        state = put_in(state.pending[node_id], %{loader: pid, waiters: [], mref: mref})
        state = put_in(state.monitors[mref], node_id)
        {:reply, :load, state}

      %{waiters: waiters} = entry ->
        ref = make_ref()
        entry = %{entry | waiters: [{pid, ref} | waiters]}
        :telemetry.execute([:upkeep, :read_nodes, :coalesced], %{count: 1}, %{node_id: node_id})
        {:reply, {:wait, ref}, put_in(state.pending[node_id], entry)}
    end
  end

  def handle_call({:pending?, node_id}, _from, state) do
    {:reply, Map.has_key?(state.pending, node_id), state}
  end

  @impl true
  def handle_cast({:settle, node_id, outcome}, state) do
    {:noreply, settle(state, node_id, outcome)}
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, mref) do
      {nil, _} ->
        {:noreply, state}

      {node_id, monitors} ->
        state = %{state | monitors: monitors}
        {:noreply, settle(state, node_id, {:loader_down, reason}, demonitor: false)}
    end
  end

  defp settle(state, node_id, outcome, opts \\ []) do
    case Map.pop(state.pending, node_id) do
      {nil, _} ->
        state

      {%{waiters: waiters, mref: mref}, pending} ->
        Enum.each(waiters, fn {pid, ref} -> send(pid, {ref, outcome}) end)

        if Keyword.get(opts, :demonitor, true) and is_reference(mref) do
          Process.demonitor(mref, [:flush])
        end

        monitors = Map.delete(state.monitors, mref)
        %{state | pending: pending, monitors: monitors}
    end
  end
end
