defmodule Upkeep.TestSupport.MultiNodeProbe do
  @moduledoc """
  Helpers callable across cluster nodes via `:erpc`.

  Test-only utilities for the multi-node coordinator tests. They live
  under `lib/` (rather than `test/support/`) so the bytecode is part
  of the `:upkeep` application and gets loaded automatically when the
  app starts on a peer node — `:erpc.call/4` cannot serialize closures
  defined in a test module the peer hasn't compiled.
  """

  @doc """
  Return the set of nodes whose pids are members of the upkeep
  notification group from the caller's view.
  """
  def notification_group_node_view do
    Upkeep.Coordinator.Graph.group()
    |> Group.members(Upkeep.Coordinator.Graph.notification_key())
    |> Enum.map(fn {pid, _meta} -> node(pid) end)
    |> Enum.uniq()
  end
end
