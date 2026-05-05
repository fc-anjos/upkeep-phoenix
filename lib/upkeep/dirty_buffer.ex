defmodule Upkeep.DirtyBuffer do
  @moduledoc """
  Threshold-driven dirty-set with deferred flush.

  Callers `enqueue/2` items as they arrive. The buffer reports back:

    * `:flush_now` once the size hits its `:threshold` — the caller drains
      and processes immediately;
    * `:schedule` the first time items arrive while idle — the caller arms
      a timer (e.g. `Process.send_after(self(), :flush, …)`);
    * `:wait` once a flush is already scheduled — the caller does nothing.

  `drain/1` returns the buffered items as a list and clears both the dirty
  set and the scheduled flag, so the next `enqueue/2` arms a fresh timer.

  Plain functional state. Owning processes hold one buffer per work class.
  """

  defstruct dirty: MapSet.new(), threshold: 1_000, scheduled?: false

  def new(opts \\ []) do
    %__MODULE__{threshold: Keyword.get(opts, :threshold, 1_000)}
  end

  def size(%__MODULE__{dirty: dirty}), do: MapSet.size(dirty)

  def empty?(%__MODULE__{dirty: dirty}), do: MapSet.size(dirty) == 0

  def members(%__MODULE__{dirty: dirty}), do: MapSet.to_list(dirty)

  @doc """
  Add `items` to the dirty set. Returns `{action, buffer}`.

    * `{:flush_now, buffer}` — threshold reached; drain immediately.
    * `{:schedule, buffer}` — first items since idle; caller schedules.
    * `{:wait, buffer}` — flush already scheduled; nothing to do.
  """
  def enqueue(%__MODULE__{} = buffer, items) do
    new_dirty = Enum.reduce(items, buffer.dirty, &MapSet.put(&2, &1))
    buffer = %{buffer | dirty: new_dirty}

    cond do
      MapSet.size(new_dirty) >= buffer.threshold -> {:flush_now, buffer}
      buffer.scheduled? -> {:wait, buffer}
      true -> {:schedule, %{buffer | scheduled?: true}}
    end
  end

  @doc """
  Pull every dirty item out as a list and clear the buffer + scheduled flag.
  """
  def drain(%__MODULE__{} = buffer) do
    {MapSet.to_list(buffer.dirty), %{buffer | dirty: MapSet.new(), scheduled?: false}}
  end
end
