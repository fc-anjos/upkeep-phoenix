defmodule Upkeep.Live.ScopeHook do
  @moduledoc false

  import Phoenix.Component, only: [assign_new: 3]

  def on_mount(:default, _params, _session, socket) do
    {:cont, assign_new(socket, :current_scope, fn -> nil end)}
  end
end
