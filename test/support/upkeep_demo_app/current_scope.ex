defmodule Upkeep.DemoAppWeb.CurrentScope do
  @moduledoc false

  import Phoenix.Component, only: [assign: 3]

  def on_mount(:default, _params, session, socket) do
    {:cont, assign(socket, :current_scope, %{user_id: user_id!(session)})}
  end

  defp user_id!(%{"user_id" => user_id}) when is_integer(user_id), do: user_id

  defp user_id!(%{"user_id" => user_id}) when is_binary(user_id) do
    String.to_integer(user_id)
  end

  defp user_id!(%{user_id: user_id}) when is_integer(user_id), do: user_id
end
