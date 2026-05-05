defmodule Upkeep.PublicApiTest do
  use ExUnit.Case, async: true

  @public_modules [
    Upkeep,
    Upkeep.Change,
    Upkeep.Ecto.Repo,
    Upkeep.Live,
    Upkeep.Observability,
    Upkeep.Source,
    Upkeep.Source.Coverage,
    Upkeep.Test
  ]

  @internal_modules [
    Upkeep.Internal.Coordinator.Graph,
    Upkeep.Internal.Coordinator.ReadNodes,
    Upkeep.Internal.Coordinator.ReadNodes.Watcher,
    Upkeep.Internal.Coordinator.Topology,
    Upkeep.Internal.DAG.Diff,
    Upkeep.Internal.DAG.Graph,
    Upkeep.Internal.DAG.Plan,
    Upkeep.Internal.DAG.Store,
    Upkeep.Internal.DirtyBuffer,
    Upkeep.Internal.Ecto.QueryDeps,
    Upkeep.ImplicitScopeError,
    Upkeep.Internal.Mutation,
    Upkeep.Internal.Retry,
    Upkeep.Internal.SingleFlight,
    Upkeep.Internal.SingleFlight.Registry,
    Upkeep.Source.Keys,
    Upkeep.Source.RepoCaptureGuard,
    Upkeep.Source.Runtime,
    Upkeep.Source.Spec,
    Upkeep.TestSupport.MultiNodeProbe
  ]

  test "stable public modules remain visible in generated docs" do
    for module <- @public_modules do
      assert documented?(module), "expected #{inspect(module)} to have public module docs"
    end
  end

  test "internal implementation modules stay hidden from generated docs" do
    for module <- @internal_modules do
      assert hidden?(module), "expected #{inspect(module)} to use @moduledoc false"
    end
  end

  test "public setup and diagnostics helpers remain exported" do
    Code.ensure_loaded!(Upkeep.Test)
    Code.ensure_loaded!(Upkeep.Change)
    Code.ensure_loaded!(Upkeep.Ecto.Repo)

    assert function_exported?(Upkeep.Test, :assert_repo_capture_enabled!, 1)
    assert function_exported?(Upkeep.Test, :assert_source_reactive!, 2)
    assert function_exported?(Upkeep.Change, :broad_update?, 1)
    assert function_exported?(Upkeep.Ecto.Repo, :capture_enabled?, 1)
  end

  defp documented?(module) do
    case module_doc(module) do
      %{"en" => doc} when is_binary(doc) and doc != "" -> true
      _ -> false
    end
  end

  defp hidden?(module), do: module_doc(module) == :hidden

  defp module_doc(module) do
    Code.ensure_loaded!(module)

    case Code.fetch_docs(module) do
      {:docs_v1, _anno, _beam_language, _format, module_doc, _metadata, _docs} -> module_doc
      {:error, reason} -> flunk("could not fetch docs for #{inspect(module)}: #{inspect(reason)}")
    end
  end
end
