defmodule Upkeep.Live.SourceRegistry do
  @moduledoc """
  In-memory map of `node_id -> source_location | nil`, fed by telemetry.

  `Upkeep.Live.{watch, derive, component}` emit `[:upkeep, :live, :registered]`
  events for every registration. The location is non-nil when the call went
  through `Upkeep.Live.Macros` (which captures `__CALLER__` at compile time)
  and nil when the runtime functions were called directly (no compile-time
  context available).

  This registry stores both kinds — including the nil-location entries — so
  the inspector can surface nodes that were registered without a captured
  callsite.
  """

  use GenServer

  @name __MODULE__
  @table __MODULE__
  @handler_id {__MODULE__, :telemetry}
  @event [:upkeep, :live, :registered]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, @name))
  end

  @doc """
  Returns `{:ok, location | nil}` if the node was registered, `:error` otherwise.

  Use this when you need to distinguish "registered without a captured callsite"
  from "never went through Upkeep.Live registration at all" (e.g. internal
  scope nodes).
  """
  def fetch(node_id) do
    case :ets.whereis(@table) do
      :undefined ->
        :error

      ref ->
        case :ets.lookup(ref, node_id) do
          [{^node_id, location}] -> {:ok, location}
          [] -> :error
        end
    end
  end

  @doc """
  Returns the source location for a node, or nil if no location was captured
  (whether the node was registered or not).
  """
  def lookup(node_id) do
    case fetch(node_id) do
      {:ok, location} -> location
      :error -> nil
    end
  end

  @doc "Forget all captured registrations. Useful in tests."
  def clear do
    GenServer.call(@name, :clear)
  end

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :public, read_concurrency: true])
    :telemetry.detach(@handler_id)

    :ok =
      :telemetry.attach(
        @handler_id,
        @event,
        &__MODULE__.handle_event/4,
        %{table: table}
      )

    {:ok, %{table: table}}
  end

  @impl true
  def terminate(_reason, _state) do
    :telemetry.detach(@handler_id)
    :ok
  end

  def handle_event(_event, _measurements, %{node_id: node_id} = metadata, %{table: table}) do
    location = Map.get(metadata, :source_location)
    :ets.insert(table, {node_id, location})
    :ok
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  @impl true
  def handle_call(:clear, _from, %{table: table} = state) do
    :ets.delete_all_objects(table)
    {:reply, :ok, state}
  end
end
