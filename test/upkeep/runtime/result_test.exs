defmodule Upkeep.Runtime.ResultTest do
  use ExUnit.Case, async: true

  alias Upkeep.Runtime
  alias Upkeep.Runtime.Specs

  import Upkeep.TestSupport, only: [attach_telemetry: 1]

  defmodule Issue do
    defstruct [:project_id]
  end

  defmodule ProjectIssues do
    use Upkeep.Source

    def load(_params), do: [:issue]

    invalidated_by(Issue, :updated, on: :project_id)
  end

  test "runtime mount returns effects instead of applying them" do
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

  test "runtime result materializes tagged effects" do
    attach_telemetry([[:upkeep, :live, :effects, :apply]])

    result =
      {:ok, new_socket(),
       [
         {:assign, :issues, [:issue]},
         {:telemetry, [:runtime, :result, :test], %{count: 1}, %{kind: :test}}
       ]}

    socket = Runtime.to_socket(result)
    assert socket.assigns.issues == [:issue]

    assert_receive {:telemetry, [:upkeep, :live, :effects, :apply],
                    %{count: 1, duration: duration},
                    %{
                      effect_count: 2,
                      assign_count: 1,
                      telemetry_count: 1,
                      register_source_count: 0
                    }}

    assert is_integer(duration)
  end

  defp find_effect(effects, kind) do
    Enum.find(effects, fn
      {^kind, _event, _measurements, _metadata} -> true
      _other -> false
    end)
  end

  defp new_socket, do: %Phoenix.LiveView.Socket{assigns: %{__changed__: %{}}}
end
