defmodule Upkeep.SingleFlight do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [Registry]

  defstruct loads: %{}, refs: %{}

  def new, do: %__MODULE__{}

  def start(%__MODULE__{} = coalescer, key, ref, from, extra \\ nil) when is_reference(ref) do
    waiters = if is_nil(from), do: [], else: [from]
    load = %{ref: ref, waiters: waiters, extra: extra}

    %{
      coalescer
      | loads: Map.put(coalescer.loads, key, load),
        refs: Map.put(coalescer.refs, ref, key)
    }
  end

  def join(%__MODULE__{} = coalescer, key, from) do
    case Map.fetch(coalescer.loads, key) do
      {:ok, load} ->
        load = %{load | waiters: [from | load.waiters]}
        {:joined, load, %{coalescer | loads: Map.put(coalescer.loads, key, load)}}

      :error ->
        :no_load
    end
  end

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

  def reply_all(%{waiters: waiters}, reply) do
    Enum.each(waiters, &GenServer.reply(&1, reply))
  end

  def demonitor_all(%__MODULE__{refs: refs}) do
    refs |> Map.keys() |> Enum.each(&Process.demonitor(&1, [:flush]))
    :ok
  end
end
