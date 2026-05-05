defmodule Upkeep.Observability do
  @moduledoc """
  Lightweight in-memory telemetry log for local development and tests.

  This is intentionally not a durable audit log. It keeps a bounded buffer of
  recent Upkeep runtime events so tooling can answer basic "why did this update?"
  questions without every caller attaching telemetry handlers by hand.
  """

  use GenServer

  @name __MODULE__
  @handler_id {__MODULE__, :telemetry}
  @default_limit 200

  @events [
    [:upkeep, :source, :watch],
    [:upkeep, :source, :coverage],
    [:upkeep, :source, :queue],
    [:upkeep, :source, :reload, :start],
    [:upkeep, :source, :reload, :stop],
    [:upkeep, :source, :reload, :exception],
    [:upkeep, :source, :unwatch],
    [:upkeep, :dag, :recompute, :start],
    [:upkeep, :dag, :recompute, :stop],
    [:upkeep, :dag, :recompute, :exception],
    [:upkeep, :live, :assign],
    [:upkeep, :derive, :sharing],
    [:upkeep, :derive, :sharing_plan],
    [:upkeep, :graph, :dispatch, :start],
    [:upkeep, :graph, :dispatch, :stop],
    [:upkeep, :graph, :dispatch, :exception],
    [:upkeep, :graph, :initial_load, :hit],
    [:upkeep, :graph, :initial_load, :miss],
    [:upkeep, :graph, :derived_initial, :hit],
    [:upkeep, :graph, :derived_initial, :miss],
    [:upkeep, :graph, :derived_initial, :exception],
    [:upkeep, :graph, :source_load, :start],
    [:upkeep, :graph, :source_load, :stop],
    [:upkeep, :graph, :source_load, :exception],
    [:upkeep, :coordinator, :dispatch, :start],
    [:upkeep, :coordinator, :dispatch, :stop],
    [:upkeep, :coordinator, :dispatch, :exception]
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, @name))
  end

  def recent(opts \\ []) do
    limit = Keyword.get(opts, :limit, @default_limit)
    GenServer.call(@name, {:recent, limit})
  end

  def clear do
    GenServer.call(@name, :clear)
  end

  @impl true
  def init(opts) do
    limit = Keyword.get(opts, :limit, @default_limit)
    :telemetry.detach(@handler_id)

    :ok =
      :telemetry.attach_many(
        @handler_id,
        @events,
        &__MODULE__.handle_event/4,
        %{server: self()}
      )

    {:ok, %{events: [], limit: limit}}
  end

  @impl true
  def terminate(_reason, _state) do
    :telemetry.detach(@handler_id)
    :ok
  end

  def handle_event(event, measurements, metadata, %{server: server}) do
    GenServer.cast(
      server,
      {:telemetry, event, measurements, metadata, System.system_time(:microsecond)}
    )
  end

  @impl true
  def handle_cast({:telemetry, event, measurements, metadata, at}, state) do
    entry = %{
      at: at,
      event: event,
      measurements: measurements,
      metadata: metadata
    }

    {:noreply, %{state | events: [entry | state.events] |> Enum.take(state.limit)}}
  end

  @impl true
  def handle_call({:recent, limit}, _from, state) do
    events =
      state.events
      |> Enum.take(limit)
      |> Enum.reverse()

    {:reply, events, state}
  end

  def handle_call(:clear, _from, state) do
    {:reply, :ok, %{state | events: []}}
  end
end
