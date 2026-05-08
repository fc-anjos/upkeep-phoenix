defmodule Upkeep.Coordinator.MultiNodeTest do
  @moduledoc """
  Real cluster validation for cross-node source-cache invalidation.

  We boot a sister BEAM via `:peer.start_link/1`, start the `:upkeep`
  application on it, and let `Group` mesh the two nodes via Erlang
  distribution. From there we can exercise the actual cluster paths
  rather than the single-node simulation that other tests rely on.

  Tests are tagged `:multi_node` so CI can opt out if `:peer` /
  distribution is unavailable. They are marked async: false because
  Erlang distribution is process-global state.
  """
  use ExUnit.Case, async: false

  @moduletag :multi_node

  alias Upkeep.Invalidation.ReadCache, as: ReadCache

  defmodule FakeSchema, do: defstruct([:id])

  setup_all do
    ensure_distributed!()
    restart_upkeep_after_distribution!()
    {:ok, peer, peer_node} = start_peer()

    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        _, _ -> :ok
      end
    end)

    %{peer: peer, peer_node: peer_node}
  end

  describe "cluster-wide ReadCache invalidation" do
    test "Invalidation.dispatch on parent evicts ReadCache entries cached on peer", %{peer: peer} do
      seed_peer_read_node(peer, FakeSchema, 1001)

      assert :erpc.call(node_of(peer), ReadCache, :count, []) == 1

      Upkeep.Invalidation.dispatch(%Upkeep.Change{
        name: :updated,
        action: :updated,
        schema: FakeSchema,
        record: %FakeSchema{id: 7}
      })

      wait_until(
        fn -> :erpc.call(node_of(peer), ReadCache, :count, []) == 0 end,
        "peer ReadCache should drain after parent notify"
      )
    end

    test "Invalidation.dispatch on peer evicts ReadCache entries cached on parent", %{peer: peer} do
      ReadCache.clear()
      seed_local_read_node(FakeSchema, 2002)
      assert ReadCache.count() == 1

      :erpc.call(node_of(peer), Upkeep.Invalidation, :dispatch, [
        %Upkeep.Change{
          name: :inserted,
          action: :inserted,
          schema: FakeSchema,
          record: %FakeSchema{id: 9}
        }
      ])

      wait_until(
        fn -> ReadCache.count() == 0 end,
        "parent ReadCache should drain after peer notify"
      )
    end
  end

  describe "cluster-wide graph dispatch" do
    test "invalidation dispatch on parent refreshes graph subscriber registered on peer", %{
      peer_node: peer_node
    } do
      node_id = {:peer_graph_source, System.unique_integer([:positive])}
      event_name = :multi_node_graph_dispatch
      value = {:loaded_on_peer, peer_node}

      {:ok, subscriber} =
        :erpc.call(peer_node, Upkeep.TestSupport.MultiNodeHarness, :start_graph_subscriber, [
          self(),
          node_id,
          event_name,
          value
        ])

      assert_receive {:peer_graph_subscriber_registered, ^peer_node, ^subscriber, ^node_id}, 1_000

      Upkeep.Invalidation.dispatch(Upkeep.Change.changed(:multi_node_graph_dispatch, %{id: 1}))

      assert_receive {:peer_dag_values, ^peer_node, ^subscriber, [{^node_id, ^value}]}, 3_000

      send(subscriber, :stop)
    end
  end

  # Discovered gap (deferred):
  #
  # **Cross-node single-flight (cluster-wide coalescing).** Each node's
  # Coalescer is independent. N concurrent cold mounts on each of K nodes
  # produce K DB hits, not 1. The fix is a Group-based leader election
  # keyed by `node_id`: first caller cluster-wide wins the load; others
  # wait via `Group.dispatch` of the settled value.

  defp seed_local_read_node(schema, fingerprint) do
    deps = %Upkeep.Ecto.Source.QueryDeps{schemas: MapSet.new([schema])}
    node_id = {:read, :synthetic_repo, fingerprint}
    :ets.insert(ReadCache.values_table(), {node_id, []})

    for action <- [:inserted, :updated, :deleted] do
      :ets.insert(ReadCache.index_table(), {{action, schema}, {node_id, deps}})
    end

    :ok
  end

  defp seed_peer_read_node(peer, schema, fingerprint) do
    peer_node = node_of(peer)
    deps = %Upkeep.Ecto.Source.QueryDeps{schemas: MapSet.new([schema])}
    node_id = {:read, :synthetic_repo, fingerprint}

    :erpc.call(peer_node, :ets, :insert, [ReadCache.values_table(), {node_id, []}])

    for action <- [:inserted, :updated, :deleted] do
      :erpc.call(peer_node, :ets, :insert, [
        ReadCache.index_table(),
        {{action, schema}, {node_id, deps}}
      ])
    end

    :ok
  end

  defp node_of(peer) do
    :peer.call(peer, :erlang, :node, [])
  end

  defp ensure_distributed! do
    if Node.alive?() do
      :ok
    else
      {:ok, _} =
        Node.start(
          :"upkeep_test_parent_#{System.unique_integer([:positive])}@127.0.0.1",
          :longnames
        )

      Node.set_cookie(:upkeep_multi_node_test)
      :ok
    end
  end

  defp restart_upkeep_after_distribution! do
    # `mix test` starts the application before this file can enable
    # distribution. Restarting gives Group local pids whose owner node is
    # the real distributed node, so its built-in peer_connect/cluster_state
    # handshake can snapshot parent state to a late-joining peer.
    :ok = Application.stop(:upkeep)
    {:ok, _started} = Application.ensure_all_started(:upkeep)
    :ok
  end

  defp start_peer do
    cookie = Node.get_cookie()

    {:ok, peer, peer_node} =
      :peer.start_link(%{
        name: :"upkeep_peer_#{System.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        connection: :standard_io,
        args: [
          ~c"-setcookie",
          Atom.to_charlist(cookie)
        ]
      })

    # Mirror parent's code paths so the peer can load `:upkeep` and deps.
    :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])

    case :peer.call(peer, :net_kernel, :start, [[peer_node, :longnames]]) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    true = :peer.call(peer, :erlang, :set_cookie, [cookie])

    # Start logger explicitly to see crashes from the peer.
    {:ok, _} = :peer.call(peer, Application, :ensure_all_started, [:logger])

    # Start `:upkeep` on the peer BEFORE connecting the cluster mesh.
    # Group's default-cluster peer discovery handles this ordering:
    # `:nodeup` triggers peer_connect/peer_connect_ack and both sides
    # exchange full cluster_state snapshots for shared clusters.
    {:ok, _started} = :peer.call(peer, Application, :ensure_all_started, [:upkeep])

    # Connect the cluster mesh — Group sees `:nodeup` on both sides
    # and exchanges `:peer_connect` + `:cluster_state` for our named
    # notification cluster. After this, pg memberships replicate
    # bidirectionally.
    true = Node.connect(peer_node)

    # Wait until both nodes can see *both* nodes in the notification
    # group. Group's pg-based replication is eventually consistent, so
    # we confirm bidirectional visibility before testing notify fanout.
    harness = Upkeep.TestSupport.MultiNodeHarness
    expected = Enum.sort([Node.self(), peer_node])

    diag = fn ->
      "local_nodes=#{inspect(Node.list())} peer_nodes=#{inspect(:erpc.call(peer_node, Node, :list, []))} " <>
        "local_view=#{inspect(harness.notification_group_node_view())} " <>
        "peer_view=#{inspect(:erpc.call(peer_node, harness, :notification_group_node_view, []))}"
    end

    wait_until(
      fn ->
        Enum.sort(harness.notification_group_node_view()) == expected and
          Enum.sort(:erpc.call(peer_node, harness, :notification_group_node_view, [])) == expected
      end,
      "both nodes should see both watchers in the notification cluster (#{diag.()})",
      10_000
    )

    {:ok, peer, peer_node}
  end

  defp wait_until(fun, msg, timeout \\ 3_000, step \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, msg, deadline, step)
  end

  defp do_wait_until(fun, msg, deadline, step) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("timed out: #{msg}")

      true ->
        Process.sleep(step)
        do_wait_until(fun, msg, deadline, step)
    end
  end
end
