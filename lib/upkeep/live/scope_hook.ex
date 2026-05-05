defmodule Upkeep.Live.ScopeHook do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3, assign_new: 3]

  @inspect_params ["_upkeep", "upkeep_inspect"]

  def on_mount(:default, params, session, socket) do
    on_mount([inspector: true], params, session, socket)
  end

  def on_mount(opts, params, _session, socket) when is_list(opts) do
    socket = assign_new(socket, :current_scope, fn -> nil end)

    if Keyword.get(opts, :inspector, true) do
      {:cont, maybe_enable_inspector(socket, params)}
    else
      {:cont, socket}
    end
  end

  defp maybe_enable_inspector(socket, params) do
    case inspector_mode(params) do
      nil ->
        socket

      mode ->
        socket
        |> assign(:upkeep_inspector?, true)
        |> assign(:upkeep_inspector_mode, mode)
        |> Phoenix.LiveView.render_with(&Upkeep.Live.Inspector.render/1)
    end
  end

  defp inspector_mode(params) when is_map(params) do
    @inspect_params
    |> Enum.find_value(&Map.get(params, &1))
    |> normalize_mode()
  end

  defp normalize_mode(value) when value in [true, "1", "true", "inspect", "inspector"],
    do: :inspect

  defp normalize_mode(value) when value in ["dag", "graph"], do: :dag
  defp normalize_mode(_value), do: nil
end
