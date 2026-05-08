defmodule Upkeep.TestSupport.LiveSocket do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  @default_endpoint UpkeepWeb.Endpoint
  @default_view UpkeepWeb.KanbanLive

  def socket(assigns \\ %{}) when is_map(assigns) do
    %Phoenix.LiveView.Socket{assigns: with_changed(assigns)}
  end

  def disconnected_socket(assigns \\ %{}) when is_map(assigns) do
    %Phoenix.LiveView.Socket{
      endpoint: @default_endpoint,
      view: @default_view,
      assigns: with_changed(assigns)
    }
  end

  def connected_socket(assigns \\ %{}) when is_map(assigns) do
    %Phoenix.LiveView.Socket{
      endpoint: @default_endpoint,
      view: @default_view,
      transport_pid: self(),
      assigns: with_changed(assigns)
    }
  end

  def scoped_connected_socket(current_scope) do
    connected_socket()
    |> with_current_scope(current_scope)
  end

  def with_current_scope(%Phoenix.LiveView.Socket{} = socket, current_scope) do
    assign(socket, :current_scope, current_scope)
  end

  defp with_changed(assigns), do: Map.put_new(assigns, :__changed__, %{})
end
