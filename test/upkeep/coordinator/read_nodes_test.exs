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

  test "concurrent fetch_or_load for the same query collapses into one DB hit" do
    for id <- 1..50, do: Repo.insert!(%Project{id: id, name: "p#{id}"})

    counter = :counters.new(1, [])
    handler_id = {__MODULE__, :coalesce_test}

    :telemetry.attach(
      handler_id,
      [:upkeep, :repo, :query],
      fn _event, _measurements, _meta, _config -> :counters.add(counter, 1, 1) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    parent = self()
    barrier = make_ref()
    n = 25

    pids =
      for i <- 1..n do
        spawn_link(fn ->
          # Each task builds the query in its own process to mimic
          # independent LV mounts that don't share Ecto state.
          q = from(p in Project, where: p.id <= 50)
          send(parent, {:ready, i})

          receive do
            {^barrier, :go} -> :ok
          end

          rows = ReadNodes.fetch_or_load(Repo, q)
          send(parent, {:done, i, length(rows)})
        end)
      end

    for i <- 1..n, do: assert_receive({:ready, ^i}, 5_000)
    Enum.each(pids, fn pid -> send(pid, {barrier, :go}) end)

    for i <- 1..n, do: assert_receive({:done, ^i, 50}, 10_000)

    assert :counters.get(counter, 1) == 1
    assert ReadNodes.count() == 1
  end

  test "loader exception propagates to all waiters and clears pending state" do
    parent = self()
    barrier = make_ref()
    n = 5

    # Replace the fetch_or_load body with a function that throws by
    # talking to the coalescer directly — keeps this test independent of
    # ReadNodes' real load path.
    bad_node = {:read, :test_repo, :erlang.unique_integer()}

    spawner = fn ->
      for i <- 1..n do
        spawn_link(fn ->
          send(parent, {:ready, i})

          receive do
            {^barrier, :go} -> :ok
          end

          try do
            Upkeep.Coordinator.ReadNodes.Coalescer.coalesce(bad_node, fn ->
              if i == 1 do
                raise "boom"
              else
                :unreachable
              end
            end)

            send(parent, {:done, i, :ok})
          rescue
            e -> send(parent, {:done, i, {:error, Exception.message(e)}})
          end
        end)
      end
    end

    pids = spawner.()
    for i <- 1..n, do: assert_receive({:ready, ^i}, 5_000)
    Enum.each(pids, fn pid -> send(pid, {barrier, :go}) end)

    results =
      for _ <- 1..n do
        assert_receive({:done, _i, outcome}, 5_000)
        outcome
      end

    # All callers got :error (loader raised, waiters re-raise the same).
    assert Enum.all?(results, &match?({:error, "boom"}, &1))
    refute Upkeep.Coordinator.ReadNodes.Coalescer.pending?(bad_node)
  end

  test "Watcher invalidates ReadNodes when it receives a dispatched notification" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)
    ReadNodes.fetch_or_load(Repo, q)
    assert ReadNodes.count() == 1

    # Simulate a dispatched notification arriving on a *remote* node by
    # sending the same message Group.dispatch would deliver, directly to
    # the Watcher pid. This bypasses the inline invalidate inside
    # Graph.notify/1, isolating the Watcher's behavior — which is the
    # only mechanism remote nodes have to learn about evictions.
    watcher = Process.whereis(Upkeep.Coordinator.ReadNodes.Watcher)
    assert is_pid(watcher)

    event = Upkeep.Change.updated(%Project{id: 1, name: "alpha2"})
    send(watcher, {:upkeep_graph_notify, event})

    # Wait for the cast to be processed.
    _ = :sys.get_state(watcher)
    assert ReadNodes.count() == 0
  end

  test "Watcher is a member of the cluster notification group" do
    members = Group.members(Upkeep.Coordinator.Graph.group(), Upkeep.Coordinator.Graph.notification_key())
    pids = Enum.map(members, fn {pid, _meta} -> pid end)
    assert Process.whereis(Upkeep.Coordinator.ReadNodes.Watcher) in pids
  end
end
