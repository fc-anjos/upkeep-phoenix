defmodule Upkeep.Change do
  @moduledoc """
  Normalized notification delivered through Upkeep.

  A change can be a record-aware convenience notification such as `:updated`, or
  an application-named semantic notification such as `:issue_moved`.
  """

  defstruct [:name, :action, :schema, :record, :from, :payload, meta: %{}]

  def inserted(record, opts \\ []) when is_struct(record) do
    new(:inserted, record, Keyword.put(opts, :action, :inserted))
  end

  def updated(record, opts \\ []) when is_struct(record) do
    new(:updated, record, Keyword.put(opts, :action, :updated))
  end

  def deleted(record, opts \\ []) when is_struct(record) do
    new(:deleted, record, Keyword.put(opts, :action, :deleted))
  end

  def changed(name, payload, opts \\ []) when is_atom(name) do
    new(name, payload, opts)
  end

  def changed?(%__MODULE__{from: nil}, _field), do: false

  def changed?(%__MODULE__{from: from, record: record}, field) when is_atom(field) do
    field_value(from, field) != field_value(record, field)
  end

  @doc """
  Returns true when an update has no old record state.

  These updates are handled as schema/action-wide invalidations because Upkeep
  cannot prove which field-indexed sources the record may have moved out of.
  """
  def broad_update?(%__MODULE__{name: :updated, from: nil}), do: true
  def broad_update?(_event), do: false

  @doc false
  def diagnose_broad_update(%__MODULE__{} = change) do
    if broad_update?(change) do
      emit_broad_update(change)
      maybe_log_broad_update(change)
    end

    :ok
  end

  def diagnose_broad_update(_event), do: :ok

  def old(%__MODULE__{from: from}, field) when is_atom(field), do: field_value(from, field)
  def new(%__MODULE__{record: record}, field) when is_atom(field), do: field_value(record, field)

  def field_sets(%__MODULE__{record: record, from: from}) do
    [record, from]
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&fields/1)
    |> Enum.uniq()
  end

  defp new(name, payload, opts) do
    record = Keyword.get(opts, :record, record_payload(payload))

    %__MODULE__{
      name: name,
      action: Keyword.get(opts, :action),
      schema: Keyword.get(opts, :schema, schema(record || payload)),
      record: record,
      from: Keyword.get(opts, :from),
      payload: payload,
      meta: Keyword.get(opts, :meta, %{})
    }
  end

  defp record_payload(%_{} = payload), do: payload
  defp record_payload(_payload), do: nil

  defp schema(%schema{}), do: schema
  defp schema(_payload), do: nil

  defp fields(%_{} = struct), do: Map.from_struct(struct)
  defp fields(map) when is_map(map), do: map
  defp fields(_term), do: %{}

  defp field_value(nil, _field), do: nil

  defp field_value(%_{} = struct, field) do
    struct
    |> Map.from_struct()
    |> Map.get(field)
  end

  defp field_value(map, field) when is_map(map), do: Map.get(map, field)
  defp field_value(_term, _field), do: nil

  defp emit_broad_update(%__MODULE__{} = change) do
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

  defp maybe_log_broad_update(%__MODULE__{} = change) do
    case broad_update_policy() do
      :warn ->
        warn_broad_update_once(change)

      :ignore ->
        :ok
    end
  end

  defp warn_broad_update_once(%__MODULE__{} = change) do
    key = {__MODULE__, :broad_update_warned}
    shape = {change.schema, change.name}
    seen = :persistent_term.get(key, MapSet.new())

    unless MapSet.member?(seen, shape) do
      :persistent_term.put(key, MapSet.put(seen, shape))

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
