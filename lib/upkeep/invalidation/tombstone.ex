defmodule Upkeep.Invalidation.Tombstone do
  @moduledoc false

  # Gossiped, per-node record of recently dispatched invalidations. A node that
  # was offline or partitioned when an invalidation fanned out never sees it,
  # leaving its read cache and source processes stale forever. On boot each node
  # asks its peers for their recent tombstones and replays the ones it missed
  # into its own invalidation bus. Entries expire on a TTL; the sweep interval is
  # deliberately shorter than the TTL so retention stays near `rate × TTL`
  # instead of drifting toward `2 × TTL`.

  use GenServer

  alias Upkeep.Invalidation.Bus

  @table :upkeep_invalidation_tombstones
  @gossip_key "invalidation/tombstone-gossip"
  @boot_sync_timeout_ms 5_000
  @default_ttl_ms 300_000
  @default_sweep_interval_ms 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def table, do: @table

  # Best-effort and hot-path safe: a missing table (server restarting) is a no-op.
  def record(event) when is_struct(event) do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      _tid ->
        :ets.insert(@table, {event, now_ms()})
        :ok
    end
  end

  def entries do
    case :ets.whereis(@table) do
      :undefined -> []
      _tid -> :ets.tab2list(@table)
    end
  end

  def purge_expired(cutoff_ms) do
    :ets.select_delete(@table, [{{:_, :"$1"}, [{:<, :"$1", cutoff_ms}], [true]}])
  end

  @impl true
  def init(opts) do
    _table =
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])

    :ok = join_gossip()
    {:ok, build_state(opts), {:continue, :boot_sync}}
  end

  @impl true
  def handle_continue(:boot_sync, state) do
    Group.dispatch(Bus.group(), @gossip_key, {:tombstone_sync_request, self()})
    collect_replies(now_ms() + state.boot_sync_timeout_ms)
    schedule_sweep(state.sweep_interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info({:tombstone_sync_request, from}, state) do
    if node(from) != node() do
      send(from, {:tombstone_sync_reply, non_expired(state.ttl_ms)})
    end

    {:noreply, state}
  end

  def handle_info({:tombstone_sync_reply, entries}, state) do
    merge_and_replay(entries)
    {:noreply, state}
  end

  def handle_info(:sweep, state) do
    purge_expired(now_ms() - state.ttl_ms)
    schedule_sweep(state.sweep_interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp collect_replies(deadline_ms) do
    timeout = deadline_ms - now_ms()

    if timeout > 0 do
      receive do
        {:tombstone_sync_reply, entries} ->
          merge_and_replay(entries)
          collect_replies(deadline_ms)
      after
        timeout -> :ok
      end
    end
  end

  # Last-writer-wins on timestamp. An event we do not already hold is one we
  # missed, so replay it locally; a known event only refreshes its timestamp.
  defp merge_and_replay(entries) do
    Enum.each(entries, fn {event, ts} ->
      case :ets.lookup(@table, event) do
        [] ->
          :ets.insert(@table, {event, ts})
          replay_local(event)

        [{_event, local_ts}] when ts > local_ts ->
          :ets.insert(@table, {event, ts})

        _kept ->
          :ok
      end
    end)
  end

  defp replay_local(event) do
    Bus.group()
    |> Group.members(Bus.key())
    |> Enum.each(fn {pid, _meta} ->
      if node(pid) == node(), do: send(pid, {:upkeep_invalidation, event})
    end)
  end

  defp non_expired(ttl_ms) do
    cutoff = now_ms() - ttl_ms
    :ets.select(@table, [{{:"$1", :"$2"}, [{:>=, :"$2", cutoff}], [{{:"$1", :"$2"}}]}])
  end

  defp join_gossip do
    case Group.join(Bus.group(), @gossip_key, %{}) do
      :ok -> :ok
      :already_joined -> :ok
    end
  end

  defp schedule_sweep(interval_ms), do: Process.send_after(self(), :sweep, interval_ms)

  defp build_state(opts) do
    %{
      ttl_ms: setting(opts, :ttl_ms, :tombstone_ttl_ms, @default_ttl_ms),
      sweep_interval_ms:
        setting(
          opts,
          :sweep_interval_ms,
          :tombstone_sweep_interval_ms,
          @default_sweep_interval_ms
        ),
      boot_sync_timeout_ms: Keyword.get(opts, :boot_sync_timeout_ms, @boot_sync_timeout_ms)
    }
  end

  defp setting(opts, opt_key, config_key, default) do
    Keyword.get(opts, opt_key) || Application.get_env(:upkeep, config_key, default)
  end

  defp now_ms, do: System.system_time(:millisecond)
end
