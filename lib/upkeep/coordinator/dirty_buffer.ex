defmodule Upkeep.Coordinator.DirtyBuffer do
  @moduledoc false

  defstruct dirty: MapSet.new(), threshold: 1_000, scheduled?: false

  def new(opts \\ []) do
    %__MODULE__{threshold: Keyword.get(opts, :threshold, 1_000)}
  end

  def size(%__MODULE__{dirty: dirty}), do: MapSet.size(dirty)

  def empty?(%__MODULE__{dirty: dirty}), do: MapSet.size(dirty) == 0

  def members(%__MODULE__{dirty: dirty}), do: MapSet.to_list(dirty)

  def put(%__MODULE__{} = buffer, items) do
    dirty = Enum.reduce(items, buffer.dirty, &MapSet.put(&2, &1))
    %{buffer | dirty: dirty}
  end

  def enqueue(%__MODULE__{} = buffer, items) do
    buffer = put(buffer, items)

    cond do
      MapSet.size(buffer.dirty) >= buffer.threshold -> {:flush_now, buffer}
      buffer.scheduled? -> {:wait, buffer}
      true -> {:schedule, %{buffer | scheduled?: true}}
    end
  end

  def drain(%__MODULE__{} = buffer) do
    {MapSet.to_list(buffer.dirty), %{buffer | dirty: MapSet.new(), scheduled?: false}}
  end
end
