defmodule Upkeep.SingleFlight do
  @moduledoc """
  In-flight call dedup for register-and-load patterns.

  When multiple callers ask for the same keyed value while a load is in
  flight, they share one task: the first call starts the load and tracks
  its monitor ref; subsequent calls `join/3` the waiter list. On result
  or DOWN, the caller `pop/2`s by ref and replies to all waiters.

  Plain functional state — the owning GenServer holds a coalescer per
  load class (e.g., one for sources, one for derived nodes).
  """

  defstruct loads: %{}, refs: %{}

  def new, do: %__MODULE__{}

  @doc """
  Register a freshly started in-flight load. The caller spawned the task
  and holds its monitor ref; opaque `extra` is stashed for use at result
  time (e.g., the `%Node{}` snapshot at load start).
  """
  def start(%__MODULE__{} = coalescer, key, ref, from, extra \\ nil) when is_reference(ref) do
    waiters = if is_nil(from), do: [], else: [from]
    load = %{ref: ref, waiters: waiters, extra: extra}

    %{
      coalescer
      | loads: Map.put(coalescer.loads, key, load),
        refs: Map.put(coalescer.refs, ref, key)
    }
  end

  @doc """
  Try to join an in-flight load for `key`. Returns `{:joined, load,
  coalescer}` if one was running, `:no_load` if the caller must `start/5`.
  """
  def join(%__MODULE__{} = coalescer, key, from) do
    case Map.fetch(coalescer.loads, key) do
      {:ok, load} ->
        load = %{load | waiters: [from | load.waiters]}
        {:joined, load, %{coalescer | loads: Map.put(coalescer.loads, key, load)}}

      :error ->
        :no_load
    end
  end

  @doc """
  Pop the load identified by `ref`. Returns `{:ok, key, load, coalescer}`
  when the ref is current; `:stale` after a duplicate result message,
  concurrent reset, or unrelated DOWN.
  """
  def pop(%__MODULE__{} = coalescer, ref) when is_reference(ref) do
    case Map.fetch(coalescer.refs, ref) do
      {:ok, key} ->
        load = Map.fetch!(coalescer.loads, key)

        coalescer = %{
          coalescer
          | loads: Map.delete(coalescer.loads, key),
            refs: Map.delete(coalescer.refs, ref)
        }

        {:ok, key, load, coalescer}

      :error ->
        :stale
    end
  end

  @doc "Reply to every waiter with the same value."
  def reply_all(%{waiters: waiters}, reply) do
    Enum.each(waiters, &GenServer.reply(&1, reply))
  end

  @doc "Demonitor every in-flight ref. Used on shard reset."
  def demonitor_all(%__MODULE__{refs: refs}) do
    refs |> Map.keys() |> Enum.each(&Process.demonitor(&1, [:flush]))
    :ok
  end
end
