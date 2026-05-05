defmodule Upkeep.Invalidation do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [
      Bus,
      ReadCache,
      SourceInvalidator
    ],
    deps: [
      Group,
      Upkeep.Change,
      Upkeep.SingleFlight,
      Upkeep.Source
    ],
    type: :strict

  use Supervisor

  alias Upkeep.Invalidation.{Bus, ReadCache}

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    Enum.each(ReadCache.table_specs(), fn {name, opts} -> ensure_table(name, opts) end)

    children = [
      {Upkeep.SingleFlight.Registry,
       name: ReadCache.coalescer_name(), telemetry_prefix: [:upkeep, :read_nodes]},
      Upkeep.Invalidation.SourceInvalidator
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def dispatch([]), do: :ok

  def dispatch(events) when is_list(events) do
    Enum.each(events, &dispatch_one/1)
    :ok
  end

  def dispatch(event) when is_struct(event), do: dispatch([event])

  def reset do
    ReadCache.clear()
  end

  defp dispatch_one(event) do
    Upkeep.Change.diagnose_broad_update(event)
    ReadCache.invalidate(event)
    Bus.dispatch(event)
  end

  defp ensure_table(name, opts) do
    case :ets.info(name) do
      :undefined -> :ets.new(name, opts)
      _ -> :ok
    end
  end
end
