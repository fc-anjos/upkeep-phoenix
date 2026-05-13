defmodule Upkeep.Invalidation.BroadUpdateDiagnostics do
  @moduledoc false

  @warn_dedup_key {__MODULE__, :broad_update_warned}

  @spec emit(term()) :: :ok
  def emit(%Upkeep.Change{} = change) do
    if Upkeep.Change.broad_update?(change) do
      emit_broad_update(change)
      maybe_log_broad_update(change)
    end

    :ok
  end

  def emit(_event), do: :ok

  defp emit_broad_update(%Upkeep.Change{} = change) do
    :telemetry.execute(
      [:upkeep, :change, :broad_update],
      %{count: 1},
      %{
        schema: change.schema,
        name: change.name,
        action: change.action,
        reason: :missing_old_state,
        policy: broad_update_policy()
      }
    )
  end

  defp maybe_log_broad_update(%Upkeep.Change{} = change) do
    case broad_update_policy() do
      :warn ->
        warn_broad_update_once(change)

      :ignore ->
        :ok
    end
  end

  defp warn_broad_update_once(%Upkeep.Change{} = change) do
    shape = {change.schema, change.name}
    seen = :persistent_term.get(@warn_dedup_key, MapSet.new())

    unless MapSet.member?(seen, shape) do
      :persistent_term.put(@warn_dedup_key, MapSet.put(seen, shape))

      require Logger

      Logger.warning(
        "Upkeep.updated/2 was notified for #{inspect(change.schema)} without `from: old_record`. " <>
          "Upkeep will refresh all matching `:updated` sources for correctness. " <>
          "Pass `from: old_record` or use `Upkeep.Ecto.Repo` capture for field-aware invalidation."
      )
    end
  end

  defp broad_update_policy do
    case Application.get_env(:upkeep, :update_without_old_state, :warn) do
      policy when policy in [:warn, :ignore] ->
        policy

      other ->
        raise ArgumentError,
              "expected :upkeep, :update_without_old_state to be :warn or :ignore, got: #{inspect(other)}"
    end
  end
end
