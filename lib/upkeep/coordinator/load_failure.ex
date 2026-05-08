defmodule Upkeep.Coordinator.LoadFailure do
  @moduledoc false

  alias Upkeep.Coordinator.Graph.Shard.Loaders
  alias Upkeep.Coordinator.{LoadedSource, Node}

  defstruct [
    :node_id,
    :node,
    :reason,
    :load_reason,
    retry_metadata: %{}
  ]

  def new(node_id, %Node{} = node, reason, load_reason, retry_metadata \\ %{}) do
    %__MODULE__{
      node_id: node_id,
      node: node,
      reason: reason,
      load_reason: load_reason,
      retry_metadata: retry_metadata
    }
  end

  def emit(state, %__MODULE__{} = failure) do
    :telemetry.execute(
      [:upkeep, :graph, :source_load, :exception],
      %{count: 1},
      telemetry_metadata(state, failure)
    )
  end

  def telemetry_metadata(state, %__MODULE__{} = failure) do
    failure.node.loader
    |> Loaders.exception_metadata(failure.reason)
    |> Map.merge(
      LoadedSource.load_metadata(
        state,
        failure.node_id,
        failure.node,
        failure.load_reason
      )
    )
    |> Map.merge(failure.retry_metadata)
  end
end
