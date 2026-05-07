defmodule Upkeep.RuntimeResultTest do
  use ExUnit.Case, async: true

  alias Upkeep.Live
  alias Upkeep.Live.Specs
  alias Upkeep.Runtime

  defmodule Issue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    def load(_params), do: [:issue]

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
    attach_telemetry([[:upkeep, :live, :effects, :apply]])

    socket =
      new_socket()
      |> Live.watch(:issues, ProjectIssues, project_id: 1)

    assert socket.assigns.issues == [:issue]

    assert_receive {:telemetry, [:upkeep, :live, :effects, :apply],
                    %{count: 1, duration: duration},
                    %{
                      effect_count: effect_count,
                      assign_count: assign_count,
                      telemetry_count: telemetry_count,
                      register_source_count: register_source_count
                    }}

    assert is_integer(duration)
    assert effect_count >= 3
    assert assign_count >= 1
    assert telemetry_count >= 1
    assert register_source_count >= 1
  end

  defp attach_telemetry(events) do
    handler_id = {__MODULE__, self(), System.unique_integer([:positive])}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, pid ->
          send(pid, {:telemetry, event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp find_effect(effects, kind) do
    Enum.find(effects, fn
      {^kind, _event, _measurements, _metadata} -> true
      _other -> false
    end)
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
end
