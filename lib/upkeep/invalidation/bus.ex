defmodule Upkeep.Invalidation.Bus do
  @moduledoc false

  @group Upkeep.Group
  @key "invalidation/notifications"

  def group, do: @group
  def key, do: @key

  def join(kind) when is_atom(kind) do
    case Group.join(@group, @key, %{kind: kind}) do
      :ok -> :ok
      :already_joined -> :ok
    end
  end

  def leave do
    case Group.leave(@group, @key) do
      :ok -> :ok
      {:error, :not_in_group} -> :ok
    end
  end

  def dispatch(event) when is_struct(event) do
    Group.dispatch(@group, @key, {:upkeep_invalidation, node(), event})
  end
end
