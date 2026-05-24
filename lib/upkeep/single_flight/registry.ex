defmodule Upkeep.SingleFlight.Registry do
  @moduledoc false

  use GenServer

  alias Upkeep.SingleFlight

  defmodule LoaderDown do
    @moduledoc false
    defexception [:key, :reason]

    @impl true
    def message(%__MODULE__{key: key, reason: reason}) do
      "Upkeep single-flight loader for #{inspect(key)} exited: #{inspect(reason)}"
    end
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec coalesce(GenServer.server(), term(), (-> term())) :: term()
  def coalesce(name, key, fun) when is_function(fun, 0) do
    case GenServer.call(name, {:reserve, key}, :infinity) do
      :load ->
        try do
          value = fun.()
          GenServer.cast(name, {:settle, key, {:ok, value}})
          value
        catch
          kind, reason ->
            stack = __STACKTRACE__
            GenServer.cast(name, {:settle, key, {:raise, kind, reason, stack}})
            :erlang.raise(kind, reason, stack)
        end

      {:wait, ref} ->
        receive do
          {^ref, {:ok, value}} ->
            value

          {^ref, {:raise, kind, reason, stack}} ->
            :erlang.raise(kind, reason, stack)

          {^ref, {:loader_down, reason}} ->
            raise LoaderDown, key: key, reason: reason
        end
    end
  end

  def pending?(name, key), do: GenServer.call(name, {:pending?, key})

  @doc false
  # Diagnostic: number of coalesced waiters currently attached to `key`.
  def waiter_count(name, key), do: GenServer.call(name, {:waiter_count, key})

  @impl true
  def init(opts) do
    {:ok,
     %{
       flight: SingleFlight.new(),
       telemetry_prefix: Keyword.get(opts, :telemetry_prefix)
     }}
  end

  @impl true
  def handle_call({:reserve, key}, {pid, _tag}, state) do
    case SingleFlight.join(state.flight, key, new_waiter(pid)) do
      {:joined, _load, flight} ->
        emit_coalesced(state.telemetry_prefix, key)

        {:reply, {:wait, elem(hd(Map.fetch!(flight.loads, key).waiters), 1)},
         %{state | flight: flight}}

      :no_load ->
        mref = Process.monitor(pid)
        {:reply, :load, %{state | flight: SingleFlight.start(state.flight, key, mref, nil)}}
    end
  end

  def handle_call({:pending?, key}, _from, state) do
    {:reply, Map.has_key?(state.flight.loads, key), state}
  end

  def handle_call({:waiter_count, key}, _from, state) do
    count =
      case Map.fetch(state.flight.loads, key) do
        {:ok, load} -> length(load.waiters)
        :error -> 0
      end

    {:reply, count, state}
  end

  @impl true
  def handle_cast({:settle, key, outcome}, state) do
    {:noreply, settle(state, key, outcome)}
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    case SingleFlight.pop(state.flight, mref) do
      {:ok, key, load, flight} ->
        # A successfully-computed result must win the race against the leader's
        # death: if the leader cast its `:settle` before exiting, that message is
        # already in our mailbox even though the `{:DOWN}` was scheduled first.
        # Drain a pending settle for this load and deliver the real outcome
        # instead of `{:loader_down, reason}` (which would lose the value and
        # raise `LoaderDown` in coalesced waiters).
        outcome = pending_settle_outcome(key, {:loader_down, reason})
        notify_waiters(load, outcome)
        {:noreply, %{state | flight: flight}}

      :stale ->
        {:noreply, state}
    end
  end

  # Selectively drain an already-enqueued `{:settle, key, outcome}` for this
  # load. `after 0` keeps it non-blocking: if no settle is buffered, the leader
  # genuinely died before producing a result, so we fall back to `default`.
  defp pending_settle_outcome(key, default) do
    receive do
      {:"$gen_cast", {:settle, ^key, outcome}} -> outcome
    after
      0 -> default
    end
  end

  # Tolerant of a load that was already terminated (e.g. by a DOWN that won the
  # race and already drained this settle); never crashes the registry.
  defp settle(state, key, outcome) do
    with {:ok, load} <- Map.fetch(state.flight.loads, key),
         ref = Map.fetch!(load, :ref),
         {:ok, ^key, load, flight} <- SingleFlight.pop(state.flight, ref) do
      Process.demonitor(ref, [:flush])
      notify_waiters(load, outcome)
      %{state | flight: flight}
    else
      _ -> state
    end
  end

  defp new_waiter(pid) do
    {pid, make_ref()}
  end

  defp notify_waiters(%{waiters: waiters}, outcome) do
    Enum.each(waiters, fn {pid, ref} -> send(pid, {ref, outcome}) end)
  end

  defp emit_coalesced(nil, _key), do: :ok

  defp emit_coalesced(prefix, key) do
    :telemetry.execute(prefix ++ [:coalesced], %{count: 1}, %{node_id: key})
  end
end
