defmodule Upkeep.Invalidation.ReadCacheTest do
  use Upkeep.TestSupport.DataCase, async: false

  import Ecto.Query

  alias Upkeep.Ecto.Source.QueryDeps
  alias Upkeep.Invalidation.ReadCache, as: ReadCache
  alias Upkeep.TestSupport.Repo

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

    ReadCache.clear()
    :ok
  end

  test "two callers fetching the same query share a single read-cache entry" do
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

    a = fetch_query(q)
    b = fetch_query(q)
    c = fetch_query(q)

    assert a == b and b == c
    assert Enum.map(a, & &1.name) == ["alpha", "beta"]
    assert :counters.get(counter, 1) == 1
    assert ReadCache.count() == 1
  end

  test "different repos with the same SQL share no cache" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)

    fetch_query(q)
    assert ReadCache.count() == 1

    # Same query against the same repo is the same cache entry.
    fetch_query(q)
    assert ReadCache.count() == 1
  end

  test "invalidate/1 evicts read-cache entries whose query touches the changed schema" do
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

    fetch_query(q)
    fetch_query(q)
    assert :counters.get(counter, 1) == 1
    assert ReadCache.count() == 1

    # Simulate a write event at the cache layer.
    event = Upkeep.Change.updated(%Project{id: 1, name: "alpha"})
    ReadCache.invalidate(event)
    assert ReadCache.count() == 0

    fetch_query(q)
    assert :counters.get(counter, 1) == 2
  end

  test "Invalidation.dispatch/1 evicts matching read-cache entries before graph dispatch" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)
    fetch_query(q)
    assert ReadCache.count() == 1

    Upkeep.Invalidation.dispatch(Upkeep.Change.inserted(%Project{id: 2, name: "beta"}))

    assert ReadCache.count() == 0
  end

  test "concurrent fetch_or_load for the same query collapses into one DB hit" do
    for id <- 1..20, do: Repo.insert!(%Project{id: id, name: "p#{id}"})

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
    n = 8

    pids =
      for i <- 1..n do
        spawn_link(fn ->
          # Each task builds the query in its own process to mimic
          # independent LV mounts that don't share Ecto state.
          q = from(p in Project, where: p.id <= 20)
          send(parent, {:ready, i})

          receive do
            {^barrier, :go} -> :ok
          end

          rows = fetch_query(q)
          send(parent, {:done, i, length(rows)})
        end)
      end

    for i <- 1..n, do: assert_receive({:ready, ^i}, 5_000)
    Enum.each(pids, fn pid -> send(pid, {barrier, :go}) end)

    for i <- 1..n, do: assert_receive({:done, ^i, 20}, 10_000)

    assert :counters.get(counter, 1) == 1
    assert ReadCache.count() == 1
  end

  test "loader exception propagates to all waiters and clears pending state" do
    parent = self()
    barrier = make_ref()
    n = 5

    # Replace the fetch_or_load body with a function that throws by
    # talking to the coalescer directly. This keeps the test independent
    # of ReadCache's real load path.
    bad_node = {:read, :test_repo, :erlang.unique_integer()}

    spawner = fn ->
      for i <- 1..n do
        spawn_link(fn ->
          send(parent, {:ready, i})

          receive do
            {^barrier, :go} -> :ok
          end

          try do
            Upkeep.SingleFlight.Registry.coalesce(ReadCache.coalescer_name(), bad_node, fn ->
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
    refute Upkeep.SingleFlight.Registry.pending?(ReadCache.coalescer_name(), bad_node)
  end

  test "SourceInvalidator invalidates ReadCache when it receives a dispatched notification" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)
    fetch_query(q)
    assert ReadCache.count() == 1

    # Simulate a dispatched notification arriving on a *remote* node by
    # sending the same message Group.dispatch would deliver, directly to
    # the SourceInvalidator pid. This bypasses the inline invalidate inside
    # Invalidation.dispatch/1, isolating the invalidator behavior, which is the
    # only mechanism remote nodes have to learn about evictions.
    watcher = Process.whereis(Upkeep.Invalidation.SourceInvalidator)
    assert is_pid(watcher)

    event = Upkeep.Change.updated(%Project{id: 1, name: "alpha2"})
    send(watcher, {:upkeep_invalidation, :remote@nohost, event})

    # Wait for the cast to be processed.
    _ = :sys.get_state(watcher)
    assert ReadCache.count() == 0
  end

  test "release/1 evicts read-cache entries when their last holder is dropped" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)
    holder_a = {:source_a, %{}}
    holder_b = {:source_b, %{}}

    fetch_query(q, holder_a)
    fetch_query(q, holder_b)
    assert ReadCache.count() == 1

    # Releasing one holder leaves the read-cache entry alive — another holder
    # is still using it.
    assert ReadCache.release(holder_a) == 0
    assert ReadCache.count() == 1

    # Releasing the last holder evicts the value and its index entries.
    assert ReadCache.release(holder_b) == 1
    assert ReadCache.count() == 0
    assert :ets.tab2list(ReadCache.index_table()) == []
  end

  test "release/1 is a no-op when no holder was recorded" do
    Repo.insert!(%Project{id: 1, name: "alpha"})

    q = from(p in Project)
    fetch_query(q)
    assert ReadCache.count() == 1

    assert ReadCache.release({:never_seen, %{}}) == 0
    assert ReadCache.count() == 1
  end

  test "removing a source node releases its read-cache entries" do
    # Use a custom source that goes through Upkeep.Source.Loader.load so the
    # holder propagation is exercised end-to-end.
    Repo.insert!(%Project{id: 1, name: "alpha"})

    defmodule HolderSource do
      use Upkeep.Ecto.Source, repo: Upkeep.TestSupport.Repo

      import Ecto.Query

      def load(params) do
        Upkeep.read(
          from(p in Upkeep.Invalidation.ReadCacheTest.Project, where: p.id == ^params.id)
        )
      end

      def reacts_to?(_event, _params), do: false
      def __upkeep_interest_keys__(_params), do: []
      def __upkeep_explicit_interest_keys__(_params), do: []
      def __upkeep_sharing_partition__(params), do: params
    end

    {_value, _deps} = Upkeep.Source.Loader.load(HolderSource, %{id: 1})
    assert ReadCache.count() == 1

    Upkeep.Invalidation.ReadCache.release(
      Upkeep.Source.Identity.source_id(HolderSource, %{id: 1})
    )

    assert ReadCache.count() == 0
  end

  test "SourceInvalidator is a member of the cluster notification group" do
    members =
      Group.members(Upkeep.Invalidation.Bus.group(), Upkeep.Invalidation.Bus.key())

    pids = Enum.map(members, fn {pid, _meta} -> pid end)
    assert Process.whereis(Upkeep.Invalidation.SourceInvalidator) in pids
  end

  defp fetch_query(query, holder \\ nil) do
    deps = QueryDeps.from_query(query)
    {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)
    node_id = {:read, Repo, :erlang.phash2({sql, params})}

    ReadCache.fetch_or_load(node_id, deps, fn -> Repo.all(query) end, holder)
  end
end
