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
end
