defmodule Upkeep.Coordinator.Stateless do
  @moduledoc """
  Stateless notify path — derives interest keys in the caller process and
  fans out via `Group.dispatch/3` directly. Exists so we can A/B against
  the singleton `Upkeep.Coordinator` under contention.
  """

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor

  def notify(event, supervisor \\ @supervisor) when is_struct(event) do
    keys =
      event
      |> Source.event_keys()
      |> Enum.map(&Source.group_key/1)
      |> Enum.uniq()

    Enum.each(keys, &Group.dispatch(supervisor, &1, {:upkeep_event, event}))
    :ok
  end
end
