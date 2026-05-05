defmodule Upkeep.Coordinator.Subscriptions do
  @moduledoc false

  @group Upkeep.Group
  @notification_key "graph/notifications"
  @source_prefix "graph/source/"

  def group, do: @group
  def notification_key, do: @notification_key

  def subscribe(node_id, meta \\ %{kind: :lv}) do
    case Group.join(@group, source_key(node_id), meta) do
      :ok -> :ok
      :already_joined -> :ok
    end
  end

  def unsubscribe(node_id) do
    case Group.leave(@group, source_key(node_id)) do
      :ok -> :ok
      {:error, :not_in_group} -> :ok
    end
  end

  def subscribers(node_id) do
    @group
    |> Group.members(source_key(node_id))
    |> Enum.map(fn {pid, _meta} -> pid end)
    |> MapSet.new()
  end

  def subscribed?(node_id, pid \\ nil) do
    pid = pid || self()
    MapSet.member?(subscribers(node_id), pid)
  end

  def join_notifications(kind) when is_atom(kind) do
    case Group.join(@group, @notification_key, %{kind: kind}) do
      :ok -> :ok
      :already_joined -> :ok
    end
  end

  def dispatch_notification(event) do
    Group.dispatch(@group, @notification_key, {:upkeep_graph_notify, node(), event})
  end

  def monitor_sources do
    Group.monitor(@group, @source_prefix)
  end

  def join_shard(idx, generation) do
    Group.join(@group, shard_key(idx), %{
      kind: :graph_shard,
      shard: idx,
      pid: self(),
      generation: generation
    })
  end

  def members(encoded_key) do
    Group.members(@group, encoded_key)
  end

  def member_count(encoded_key) do
    Group.member_count(@group, encoded_key)
  end

  def source_key(node_id) do
    @source_prefix <>
      (node_id |> :erlang.term_to_binary() |> Base.url_encode64(padding: false))
  end

  def decode_source_key(<<@source_prefix, encoded::binary>>) do
    encoded
    |> Base.url_decode64!(padding: false)
    |> :erlang.binary_to_term()
  end

  def shard_key(idx), do: "graph/shard/#{idx}"
end
