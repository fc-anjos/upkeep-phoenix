defmodule Upkeep.Internal.RuntimeResultTest do
  use ExUnit.Case, async: true

  alias Upkeep.Live
  alias Upkeep.Live.Specs
  alias Upkeep.Internal.Runtime

  defmodule Issue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    query(fn _params -> [:issue] end)

    invalidated_by(Issue, :updated, on: :project_id)
  end

  test "runtime mount returns tagged tuple effects instead of applying them" do
    socket = new_socket()
    spec = Specs.source(:issues, ProjectIssues, %{project_id: 1}, nil)

    assert {:ok, socket, effects} = Runtime.mount(socket, spec)

    source_id = {ProjectIssues, %{project_id: 1}}
    assert {:assign, :issues, [:issue]} in effects

    assert {:telemetry, [:source, :watch], %{count: 1}, metadata} =
             find_effect(effects, :telemetry)

    assert metadata.source_id == source_id
    refute Map.has_key?(socket.assigns, :issues)
  end

  test "Live materializes runtime result effects at the boundary" do
    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue]
  end

  defp find_effect(effects, kind) do
    Enum.find(effects, fn
      {^kind, _event, _measurements, _metadata} -> true
      _other -> false
    end)
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
end
