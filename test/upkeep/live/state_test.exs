defmodule Upkeep.Runtime.StateTest do
  use ExUnit.Case, async: true

  alias Upkeep.Change
  alias Upkeep.InvalidationSurface
  alias Upkeep.Runtime.State
  alias Upkeep.TestSupport.LiveSocket

  defmodule Event do
    defstruct [:scope]
  end

  defmodule OtherEvent do
    defstruct [:scope]
  end

  defmodule Record do
    defstruct [:scope, :group]
  end

  test "stores Upkeep runtime as one socket-private value" do
    socket =
      LiveSocket.socket()
      |> State.put_watch(:source_id, %{assign_names: MapSet.new([:items])})
      |> State.put_assign_node(:items, {:source, :source_id})
      |> State.queue_refresh(:source_id)

    assert %State{} = runtime = socket.private.upkeep_runtime
    assert Map.has_key?(socket.private, :upkeep_runtime)
    refute Map.has_key?(socket.private, :upkeep_watches)
    refute Map.has_key?(socket.private, :upkeep_assign_nodes)
    refute Map.has_key?(socket.private, :upkeep_pending_refreshes)
    assert Map.has_key?(runtime.watches, :source_id)
    assert runtime.assign_nodes.items == {:source, :source_id}
    assert MapSet.member?(runtime.pending_refreshes, :source_id)
  end

  test "returns empty runtime state for untouched sockets" do
    socket = LiveSocket.socket()

    assert State.watches(socket) == %{}
    assert State.assign_nodes(socket) == %{}
    assert State.pending_refreshes(socket) == MapSet.new()
    assert %Upkeep.DAG.Store{} = State.store(socket)
  end

  test "matching_watches returns exact indexed candidates and conservative unindexed candidates" do
    matching_surface = event_scope_surface(Event, :matching)
    indexed_miss_surface = event_scope_surface(Event, :other)
    other_event_surface = event_scope_surface(OtherEvent, :matching)

    unindexed_surface =
      InvalidationSurface.manual([:custom_unindexed_key], fn
        %Event{scope: :matching} -> true
        _event -> false
      end)

    socket =
      LiveSocket.socket()
      |> State.put_watch(:matching, %{surface: matching_surface})
      |> State.put_watch(:indexed_miss, %{surface: indexed_miss_surface})
      |> State.put_watch(:other_event, %{surface: other_event_surface})
      |> State.put_watch(:unindexed, %{surface: unindexed_surface})

    matches =
      socket
      |> State.matching_watches(%Event{scope: :matching})
      |> Map.new()

    assert Map.has_key?(matches, :matching)
    assert Map.has_key?(matches, :unindexed)
    refute Map.has_key?(matches, :indexed_miss)
    refute Map.has_key?(matches, :other_event)
  end

  test "put_existing_watch updates the matching_watches index" do
    old_surface = event_scope_surface(Event, :old)
    new_surface = event_scope_surface(Event, :matching)

    socket =
      LiveSocket.socket()
      |> State.put_watch(:source, %{surface: old_surface})

    assert State.matching_watches(socket, %Event{scope: :matching}) == []

    socket = State.put_existing_watch(socket, :source, %{surface: new_surface})

    assert [{:source, %{surface: ^new_surface, source_id: :source}}] =
             State.matching_watches(socket, %Event{scope: :matching})
  end

  test "broad updates return all candidates for a matching notification" do
    socket =
      LiveSocket.socket()
      |> State.put_watch(:one, %{surface: updated_record_surface(:one)})
      |> State.put_watch(:two, %{surface: updated_record_surface(:two)})
      |> State.put_watch(:other_event, %{surface: event_scope_surface(Event, :one)})

    matches =
      socket
      |> State.matching_watches(Change.updated(%Record{scope: :one}))
      |> Map.new()

    assert Map.has_key?(matches, :one)
    assert Map.has_key?(matches, :two)
    refute Map.has_key?(matches, :other_event)
  end

  test "exact updates return old and new field candidates" do
    socket =
      LiveSocket.socket()
      |> State.put_watch(:old, %{surface: updated_record_surface(:old)})
      |> State.put_watch(:new, %{surface: updated_record_surface(:new)})
      |> State.put_watch(:other, %{surface: updated_record_surface(:other)})

    matches =
      socket
      |> State.matching_watches(Change.updated(%Record{scope: :new}, from: %Record{scope: :old}))
      |> Map.new()

    assert Map.has_key?(matches, :old)
    assert Map.has_key?(matches, :new)
    refute Map.has_key?(matches, :other)
  end

  test "partial updates narrow unchanged watched fields and widen changed watched fields" do
    socket =
      LiveSocket.socket()
      |> State.put_watch(:one, %{surface: updated_record_surface(:one)})
      |> State.put_watch(:two, %{surface: updated_record_surface(:two)})

    unchanged_scope_matches =
      socket
      |> State.matching_watches(
        Change.updated(%Record{scope: :one, group: :new}, changed_fields: [:group])
      )
      |> Map.new()

    changed_scope_matches =
      socket
      |> State.matching_watches(Change.updated(%Record{scope: :one}, changed_fields: [:scope]))
      |> Map.new()

    assert Map.has_key?(unchanged_scope_matches, :one)
    refute Map.has_key?(unchanged_scope_matches, :two)

    assert Map.has_key?(changed_scope_matches, :one)
    assert Map.has_key?(changed_scope_matches, :two)
  end

  defp event_scope_surface(event_module, scope) do
    InvalidationSurface.manual([{:upkeep_event, event_module, [scope: scope]}], fn event ->
      match?(%{__struct__: ^event_module, scope: ^scope}, event)
    end)
  end

  defp updated_record_surface(scope) do
    params = %{scope: scope}

    InvalidationSurface.manual([{:upkeep_change, :updated, Record, [scope: scope]}], fn
      %Change{} = change ->
        InvalidationSurface.matches_notification?(change, %{name: :updated, schema: Record}) and
          InvalidationSurface.equal_fields?(change, params, [:scope], [:scope])

      _event ->
        false
    end)
  end
end
