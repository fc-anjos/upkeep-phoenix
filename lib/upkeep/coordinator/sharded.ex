defmodule Upkeep.Coordinator.Sharded do
  @moduledoc """
  Sharded notify path with per-recipient pid de-dup, event-level coalescing,
  and bounded-buffer backpressure.

  Each shard is a plain GenServer with a buffer. Publishers route by
  `event.__struct__` (or `Change.schema`+`Change.name`) so events of the
  same shape always land on the same shard — preserving FIFO per shape and
  concentrating coalescing opportunities.

  ## Backpressure

  Publishers cast on the fast path. If the shard's mailbox exceeds
  `@backpressure_threshold` the publisher switches to a synchronous call,
  which queues at the mailbox tail and forces the publisher to wait until
  the shard processes it. This bounds shard memory at a small multiple of
  the threshold while keeping the cast fast path under normal load.

  Publishers are never silently dropped — they either cast successfully or
  block waiting for a call reply.

  ## Correctness contract (see `Upkeep.ChangeEqualityTest`)

  Coalescing collapses `==`-equal events in a flush window. Safe iff:
  1. Subscribers treat events as reload triggers, not authoritative deltas.
  2. No code relies on cross-shape ordering (different shards may flush
     out of relative publish order).
  """

  use GenServer

  @backpressure_threshold 5_000

  def start_link(opts \\ []) do
    shards = Keyword.get(opts, :shards, System.schedulers_online())
    GenServer.start_link(__MODULE__, shards, name: __MODULE__)
  end

  def notify(event) when is_struct(event) do
    shards = :persistent_term.get({__MODULE__, :shards})
    idx = :erlang.phash2(shard_key(event), shards)
    name = shard_name(idx)
    pid = Process.whereis(name)

    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, len} when len > @backpressure_threshold ->
        GenServer.call(pid, {:notify, event}, 30_000)

      _ ->
        GenServer.cast(pid, {:notify, event})
    end
  end

  def shard_name(idx), do: :"#{__MODULE__}.Shard.#{idx}"

  @doc "Synchronously drain all shards. Returns once every shard's buffer is empty."
  def drain do
    shards = :persistent_term.get({__MODULE__, :shards})
    for idx <- 0..(shards - 1), do: GenServer.call(shard_name(idx), :drain, 60_000)
    :ok
  end

  @impl true
  def init(shards) do
    children =
      for idx <- 0..(shards - 1) do
        Supervisor.child_spec(
          {__MODULE__.Shard, name: shard_name(idx)},
          id: {__MODULE__.Shard, idx}
        )
      end

    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)
    :persistent_term.put({__MODULE__, :shards}, shards)
    {:ok, %{sup: sup, shards: shards}}
  end

  defp shard_key(%Upkeep.Change{schema: schema, name: name}), do: {schema, name}
  defp shard_key(event), do: event.__struct__

  defmodule Shard do
    @moduledoc false
    use GenServer

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
      buffer
      |> Enum.reverse()
      |> Enum.uniq()
      |> Enum.each(&dispatch/1)

      %{state | buffer: [], buffer_size: 0, flush_scheduled?: false}
    end

    defp dispatch(event) do
      pids =
        event
        |> Source.event_keys()
        |> Enum.map(&Source.group_key/1)
        |> Enum.uniq()
        |> Enum.flat_map(&Group.members(@supervisor, &1))
        |> Enum.map(fn {pid, _meta} -> pid end)
        |> Enum.uniq()

      msg = {:upkeep_event, event}
      Enum.each(pids, &send(&1, msg))
    end
  end
end
