defmodule Upkeep.Coordinator.Supervisor do
  @moduledoc false

  use Supervisor

  alias Upkeep.Coordinator.Shards
  alias Upkeep.Coordinator.Topology

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    shards = Keyword.get(opts, :shards, System.schedulers_online())
    Topology.put_shard_count(shards)
    Topology.init_tables()

    children =
      [
        Upkeep.Coordinator.Graph.Notifier,
        {Task.Supervisor, name: Shards.task_sup()}
      ] ++ shard_child_specs(shards)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp shard_child_specs(shards) do
    for idx <- 0..(shards - 1) do
      Supervisor.child_spec(
        {Upkeep.Coordinator.Graph.Shard, name: Shards.name(idx), idx: idx},
        id: {Upkeep.Coordinator.Graph.Shard, idx}
      )
    end
  end
end
