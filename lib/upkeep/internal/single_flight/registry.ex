defmodule Upkeep.Internal.SingleFlight.Registry do
  @moduledoc false

  use GenServer

  alias Upkeep.Internal.SingleFlight

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

  @doc """
  Run `fun` exactly once across concurrent callers for `key`.
  """
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

  @doc false
  def pending?(name, key), do: GenServer.call(name, {:pending?, key})

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

  @impl true
  def handle_cast({:settle, key, outcome}, state) do
    {:noreply, settle(state, key, outcome)}
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    case SingleFlight.pop(state.flight, mref) do
      {:ok, _key, load, flight} ->
        notify_waiters(load, {:loader_down, reason})
        {:noreply, %{state | flight: flight}}

      :stale ->
        {:noreply, state}
    end
  end

  defp settle(state, key, outcome) do
    ref =
      state.flight.loads
      |> Map.fetch!(key)
      |> Map.fetch!(:ref)

    case SingleFlight.pop(state.flight, ref) do
      {:ok, ^key, load, flight} ->
        Process.demonitor(ref, [:flush])
        notify_waiters(load, outcome)
        %{state | flight: flight}

      _ ->
        state
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
