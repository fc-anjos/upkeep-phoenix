defmodule Upkeep.Coordinator.Sharded.LoadOnce do
  @moduledoc """
  Sharded coordinator variant that loads the source value ONCE per
  coalesced event, then pushes the value to all interested subscribers.

  Subscribers receive `{:upkeep_value, event, value}` and assign without
  hitting the DB. Compared to the message-only `Sharded` coordinator, this
  cuts query count from `O(unique_events × subscribers)` down to
  `O(unique_events)` — the load-once-fan-out-many pattern.

  Dispatch runs in parallel tasks under a `Task.Supervisor` so a slow
  load_fn (e.g. a real DB query) doesn't block the shard from processing
  more notifies.

  The load function is registered globally via `register_load_fn/1`. For
  production this would be per-source; the bench uses a single fn.
  """

  use GenServer

  @backpressure_threshold 5_000

  def start_link(opts \\ []) do
    shards = Keyword.get(opts, :shards, System.schedulers_online())
    GenServer.start_link(__MODULE__, shards, name: __MODULE__)
  end

  def register_load_fn(fun) when is_function(fun, 1) do
    :persistent_term.put({__MODULE__, :load_fn}, fun)
  end

  def notify(event) when is_struct(event) do
    shards = :persistent_term.get({__MODULE__, :shards})
    idx = :erlang.phash2(shard_key(event), shards)
    pid = Process.whereis(shard_name(idx))

    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, len} when len > @backpressure_threshold ->
        GenServer.call(pid, {:notify, event}, 30_000)

      _ ->
        GenServer.cast(pid, {:notify, event})
    end
  end

  def shard_name(idx), do: :"#{__MODULE__}.Shard.#{idx}"
  def task_sup, do: :"#{__MODULE__}.TaskSup"

  def drain do
    shards = :persistent_term.get({__MODULE__, :shards})
    for idx <- 0..(shards - 1), do: GenServer.call(shard_name(idx), :drain, 60_000)
    # Tasks may still be running; wait briefly.
    case Task.Supervisor.children(task_sup()) do
      [] -> :ok
      children -> Enum.each(children, &Process.monitor/1)
    end

    :ok
  end

  @impl true
  def init(shards) do
    children = [
      {Task.Supervisor, name: task_sup()}
      | for idx <- 0..(shards - 1) do
          Supervisor.child_spec(
            {__MODULE__.Shard, name: shard_name(idx)},
            id: {__MODULE__.Shard, idx}
          )
        end
    ]

    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)
    :persistent_term.put({__MODULE__, :shards}, shards)
    {:ok, %{sup: sup, shards: shards}}
  end

  defp shard_key(%Upkeep.Change{schema: schema, name: name}), do: {schema, name}
  defp shard_key(event), do: event.__struct__

  defmodule Shard do
    @moduledoc false
    use GenServer

    alias Upkeep.Coordinator.Sharded.LoadOnce
    alias Upkeep.Source

    @supervisor Upkeep.DurableSupervisor
    @flush_interval_ms 1
    @flush_threshold 1_000

    def start_link(opts), do: GenServer.start_link(__MODULE__, :ok, opts)

    @impl true
    def init(:ok) do
      {:ok, %{buffer: [], buffer_size: 0, flush_scheduled?: false}}
    end

    @impl true
    def handle_cast({:notify, event}, state), do: {:noreply, enqueue(state, event)}

    @impl true
    def handle_call({:notify, event}, _from, state), do: {:reply, :ok, enqueue(state, event)}

    @impl true
    def handle_call(:drain, _from, state), do: {:reply, :ok, do_flush(state)}

    @impl true
    def handle_info(:flush, state), do: {:noreply, do_flush(state)}

    defp enqueue(state, event) do
      state = %{state | buffer: [event | state.buffer], buffer_size: state.buffer_size + 1}

      cond do
        state.buffer_size >= @flush_threshold ->
          do_flush(state)

        state.flush_scheduled? ->
          state

        true ->
          Process.send_after(self(), :flush, @flush_interval_ms)
          %{state | flush_scheduled?: true}
      end
    end

    defp do_flush(%{buffer: []} = state), do: %{state | flush_scheduled?: false}

    defp do_flush(%{buffer: buffer} = state) do
      load_fn = :persistent_term.get({LoadOnce, :load_fn})

      buffer
      |> Enum.reverse()
      |> Enum.uniq()
      |> Enum.each(fn event ->
        Task.Supervisor.start_child(LoadOnce.task_sup(), fn -> dispatch(event, load_fn) end)
      end)

      %{state | buffer: [], buffer_size: 0, flush_scheduled?: false}
    end

    defp dispatch(event, load_fn) do
      pids =
        event
        |> Source.event_keys()
        |> Enum.map(&Source.group_key/1)
        |> Enum.uniq()
        |> Enum.flat_map(&Group.members(@supervisor, &1))
        |> Enum.map(fn {pid, _meta} -> pid end)
        |> Enum.uniq()

      case pids do
        [] ->
          :ok

        _ ->
          value = load_fn.(event)
          msg = {:upkeep_value, event, value}
          Enum.each(pids, &send(&1, msg))
      end
    end
  end
end
