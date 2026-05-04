defmodule Upkeep.Live.Subscriptions do
  @moduledoc false

  alias Upkeep.Source

  @supervisor Upkeep.DurableSupervisor

  def join_interest(interest_keys, assign_name, source) do
    for key <- interest_keys do
      :ok =
        Group.join(@supervisor, Source.group_key(key), %{
          assign: assign_name,
          source: inspect(source)
        })
    end
  end

  def leave_interest(interest_keys) do
    for key <- interest_keys do
      case Group.leave(@supervisor, Source.group_key(key)) do
        :ok -> :ok
        {:error, :not_in_group} -> :ok
      end
    end

    :ok
  end

  def unused_interest_keys(interest_keys, watches) do
    remaining_keys =
      watches
      |> Map.values()
      |> Enum.flat_map(& &1.interest_keys)
      |> MapSet.new()

    Enum.reject(interest_keys, &MapSet.member?(remaining_keys, &1))
  end

  def register_interest?(%Phoenix.LiveView.Socket{endpoint: nil, view: nil}), do: true

  def register_interest?(%Phoenix.LiveView.Socket{} = socket),
    do: Phoenix.LiveView.connected?(socket)
end
