defmodule Upkeep.Live.Inspector.Format do
  @moduledoc false

  alias Upkeep.Introspection

  def assign_label([]), do: "none"
  def assign_label(assigns), do: assigns |> Enum.map(&"@#{&1}") |> Enum.join(", ")

  def join_or_empty([]), do: "empty"
  def join_or_empty(values), do: Enum.join(values, ", ")

  def set_label(set) do
    set
    |> MapSet.to_list()
    |> Enum.map(&Introspection.term_label/1)
    |> join_or_empty()
  end

  def shape_label(shape), do: Introspection.shape_label(shape)

  def truncate(nil, _max), do: ""
  def truncate(value, max) when byte_size(value) <= max, do: value
  def truncate(value, max), do: binary_part(value, 0, max - 1) <> "..."

  def scope_label(:shared), do: "shared"
  def scope_label(:local), do: "local"
  def scope_label(:unknown), do: "unspecified"
  def scope_label(other), do: to_string(other)

  def state_label(:changed_root), do: "changed root"
  def state_label(:recompute), do: "recomputed"
  def state_label(:changed), do: "recomputed, value changed"
  def state_label(:skipped), do: "skipped"
  def state_label(:cold), do: "cold"
  def state_label(:idle), do: "idle"

  def state_active?(state), do: state in [:changed_root, :recompute, :changed]
end
