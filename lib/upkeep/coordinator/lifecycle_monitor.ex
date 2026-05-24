defmodule Upkeep.Coordinator.LifecycleMonitor do
  @moduledoc false

  # Reclaims source/derived graph resources when a subscribed process dies
  # without calling `Graph.unregister/1`.

  use GenServer

  alias Upkeep.Coordinator.DerivedProcesses
  alias Upkeep.Coordinator.SourceProcesses
  alias Upkeep.Coordinator.Subscriptions

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def drain do
    if Process.whereis(__MODULE__) do
      :sys.get_state(__MODULE__)
    end

    :ok
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init(_opts) do
    :ok = Subscriptions.monitor_sources()
    {:ok, %{}}
  end

  @impl true
  def handle_info({:group, events, _info}, state) do
    events
    |> Enum.filter(&(&1.type == :left))
    |> Enum.map(& &1.key)
    |> Enum.uniq()
    |> Enum.each(&handle_left/1)

    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp handle_left(encoded_key) do
    case decode(encoded_key) do
      {:ok, node_id} -> cleanup(encoded_key, node_id)
      :error -> :ok
    end
  end

  defp cleanup(encoded_key, node_id) do
    # Read-cache holders use the same source identity as graph subscriptions.
    Upkeep.Invalidation.release_read_holder(node_id)

    if Subscriptions.member_count(encoded_key) == 0 do
      SourceProcesses.release(node_id)
      DerivedProcesses.release(node_id)
    end

    :ok
  end

  defp decode(encoded_key) do
    {:ok, Subscriptions.decode_source_key(encoded_key)}
  rescue
    _ -> :error
  end
end
