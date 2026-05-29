defmodule Upkeep.Invalidation do
  @moduledoc false

  use Boundary,
    top_level?: true,
    exports: [],
    deps: [
      Group,
      Logger,
      Upkeep.Change,
      Upkeep.ETS.TableOwner,
      Upkeep.InvalidationSurface,
      Upkeep.Source,
      Upkeep.Source.Dependencies,
      Upkeep.SingleFlight
    ],
    type: :strict

  use Supervisor

  alias Upkeep.Invalidation.{BroadUpdateDiagnostics, Bus, ReadCache, Tombstone}

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    # Read-cache tables are owned by a dedicated owner/heir process pair (started
    # FIRST) instead of by this supervisor process, so an owner crash transfers
    # the tables to its heir and they are handed back to the restarted owner
    # rather than wiping cached state. See `Upkeep.ETS.TableOwner`.
    table_owner =
      Upkeep.ETS.TableOwner.child_specs(
        name: Upkeep.Invalidation.ReadCache.TableOwner,
        tables: ReadCache.table_specs()
      )

    children =
      table_owner ++
        [
          {Upkeep.SingleFlight.Registry,
           name: ReadCache.coalescer_name(), telemetry_prefix: [:upkeep, :read_nodes]},
          Upkeep.Invalidation.SourceInvalidator,
          Upkeep.Invalidation.Tombstone
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

  def group, do: Bus.group()

  def notification_key, do: Bus.key()

  def join_notifications(kind), do: Bus.join(kind)
  def leave_notifications, do: Bus.leave()

  def fetch_read(node_id, deps, load, holder \\ nil) do
    ReadCache.fetch_or_load(node_id, deps, load, holder)
  end

  def release_read_holder(holder), do: ReadCache.release(holder)

  defp dispatch_one(event) do
    BroadUpdateDiagnostics.emit(event)
    ReadCache.invalidate(event)
    Tombstone.record(event)
    Bus.dispatch(event)
  end
end
