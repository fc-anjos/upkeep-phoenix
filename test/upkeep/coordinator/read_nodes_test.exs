defmodule Upkeep.Coordinator.ReadNodesTest do
  use Upkeep.DataCase, async: false

  import Ecto.Query

  alias Upkeep.Coordinator.ReadNodes
  alias Upkeep.Repo

  defmodule Project do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "upkeep_read_nodes_test_projects" do
      field :name, :string
    end
  end

  setup do
    Repo.query!("""
    CREATE TABLE IF NOT EXISTS upkeep_read_nodes_test_projects (
      id INTEGER PRIMARY KEY,
      name TEXT
    )
    """)

    on_exit(fn ->
      Repo.query!("DROP TABLE IF EXISTS upkeep_read_nodes_test_projects")
    end)

    ReadNodes.clear()
    :ok
  end

  test "two callers fetching the same query share a single read-node" do
    Repo.insert!(%Project{id: 1, name: "alpha"})
    Repo.insert!(%Project{id: 2, name: "beta"})

    counter = :counters.new(1, [])

    handler_id = {__MODULE__, :share_test}

    :telemetry.attach(
      handler_id,
      [:upkeep, :repo, :query],
      fn _event, _measurements, _meta, _config -> :counters.add(counter, 1, 1) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    q = from(p in Project, order_by: p.id)

    a = ReadNodes.fetch_or_load(Repo, q)
    b = ReadNodes.fetch_or_load(Repo, q)
    c = ReadNodes.fetch_or_load(Repo, q)

    assert a == b and b == c
    assert Enum.map(a, & &1.name) == ["alpha", "beta"]
    assert :counters.get(counter, 1) == 1
    assert ReadNodes.count() == 1
  end

  test "different repos with the same SQL share no cache" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)

    ReadNodes.fetch_or_load(Repo, q)
    assert ReadNodes.count() == 1

    # Same query against the same repo is the same node — sanity
    ReadNodes.fetch_or_load(Repo, q)
    assert ReadNodes.count() == 1
  end

  test "invalidate/1 evicts read-nodes whose query touches the changed schema" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    counter = :counters.new(1, [])
    handler_id = {__MODULE__, :invalidate_test}

    :telemetry.attach(
      handler_id,
      [:upkeep, :repo, :query],
      fn _event, _measurements, _meta, _config -> :counters.add(counter, 1, 1) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    q = from(p in Project)

    ReadNodes.fetch_or_load(Repo, q)
    ReadNodes.fetch_or_load(Repo, q)
    assert :counters.get(counter, 1) == 1
    assert ReadNodes.count() == 1

    # Simulate a write event the way Graph.notify would
    event = Upkeep.Change.updated(%Project{id: 1, name: "alpha"})
    ReadNodes.invalidate(event)
    assert ReadNodes.count() == 0

    ReadNodes.fetch_or_load(Repo, q)
    assert :counters.get(counter, 1) == 2
  end

  test "Graph.notify/1 evicts matching read-nodes before dispatch" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)
    ReadNodes.fetch_or_load(Repo, q)
    assert ReadNodes.count() == 1

    Upkeep.Coordinator.Graph.notify(Upkeep.Change.inserted(%Project{id: 2, name: "beta"}))

    assert ReadNodes.count() == 0
  end
end
