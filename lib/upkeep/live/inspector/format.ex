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

  def scope_class(:shared), do: "teal"
  def scope_class(:local), do: "orange"
  def scope_class(_scope), do: "gray"

  def state_label(:changed_root), do: "changed root"
  def state_label(:recompute), do: "recomputed"
  def state_label(:changed), do: "recomputed, value changed"
  def state_label(:skipped), do: "skipped"
  def state_label(:cold), do: "cold"
  def state_label(:idle), do: "idle"

  def state_class(:changed_root), do: "blue"
  def state_class(:recompute), do: "amber"
  def state_class(:changed), do: "green"
  def state_class(:skipped), do: "gray"
  def state_class(:cold), do: "gray"
  def state_class(:idle), do: "gray"

  def reason_class(:changed_root), do: "blue"
  def reason_class(:recompute), do: "amber"
  def reason_class(:changed), do: "green"
  def reason_class(:skipped), do: "gray"
  def reason_class(:cold), do: "gray"
  def reason_class(:idle), do: "gray"
end
