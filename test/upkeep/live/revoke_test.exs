defmodule Upkeep.Live.RevokeTest do
  use ExUnit.Case, async: false

  alias Upkeep.Live
  alias Upkeep.Runtime.State

  import Phoenix.Component, only: [assign: 3]
  import Upkeep.TestSupport.LiveSocket, only: [scoped_connected_socket: 1]

  defmodule Issue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    def load(params), do: [params.project_id]

    invalidated_by(Issue, :updated, on: :project_id)
  end

  test "revoke_authorization drops only watches tagged with the revoked identity" do
    socket =
      %{user_id: 1}
      |> scoped_connected_socket()
      |> Live.watch(:first, ProjectIssues, project_id: 1)
      |> assign(:current_scope, %{user_id: 2})
      |> Live.watch(:second, ProjectIssues, project_id: 2)

    assert map_size(State.watches(socket)) == 2

    socket = Live.revoke_authorization(socket, %{user_id: 999})
    assert map_size(State.watches(socket)) == 2

    socket = Live.revoke_authorization(socket, %{user_id: 1})

    assert [{ProjectIssues, %{project_id: 2}}] = Map.keys(State.watches(socket))
  end

  test "revoke_authorization accepts a predicate over the authorizing identity" do
    socket =
      %{user_id: 7}
      |> scoped_connected_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    socket = Live.revoke_authorization(socket, fn %{user_id: id} -> id == 7 end)

    assert State.watches(socket) == %{}
  end
end
