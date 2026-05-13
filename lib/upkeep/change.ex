defmodule Upkeep.Change do
  @moduledoc """
  Normalized notification delivered through Upkeep.

  A change can be a record-aware convenience notification such as `:updated`, or
  an application-named semantic notification such as `:issue_moved`.

  `updated(record, from: old_record)` has full field-change knowledge.
  `updated(record, changed_fields: fields)` has partial field-change knowledge
  from the write boundary. `updated(record)` without either is treated as a
  broad `:updated` invalidation because Upkeep cannot know which fields changed.
  """

  use Boundary,
    top_level?: true

  defstruct [:name, :action, :schema, :record, :from, :payload, :changed_fields, meta: %{}]

  @type field :: atom()
  @type field_status :: :changed | :unchanged | :unknown
  @type t :: %__MODULE__{
          name: atom(),
          action: atom() | nil,
          schema: module() | nil,
          record: struct() | map() | nil,
          from: struct() | map() | nil,
          payload: term(),
          changed_fields: [field()] | nil,
          meta: map()
        }

  @spec inserted(struct(), keyword()) :: t()
  def inserted(record, opts \\ []) when is_struct(record) do
    new(:inserted, record, Keyword.put(opts, :action, :inserted))
  end

  @spec updated(struct(), keyword()) :: t()
  def updated(record, opts \\ []) when is_struct(record) do
    new(:updated, record, Keyword.put(opts, :action, :updated))
  end

  @spec deleted(struct(), keyword()) :: t()
  def deleted(record, opts \\ []) when is_struct(record) do
    new(:deleted, record, Keyword.put(opts, :action, :deleted))
  end

  @spec changed(atom(), term(), keyword()) :: t()
  def changed(name, payload, opts \\ []) when is_atom(name) do
    new(name, payload, opts)
  end

  @spec changed?(t(), field()) :: boolean()
  def changed?(%__MODULE__{} = change, field) when is_atom(field),
    do: field_change(change, field) == :changed

  @doc """
  Returns what Upkeep knows about a field for this change.

  `:changed` means the field is known to have changed, `:unchanged` means the
  current record value is safe to use for equality matching, and `:unknown`
  means an update happened without enough field knowledge.
  """
  @spec field_change(t(), field()) :: field_status()
  def field_change(%__MODULE__{name: :updated, from: from, record: record}, field)
      when is_atom(field) and not is_nil(from) do
    if field_value(from, field) == field_value(record, field), do: :unchanged, else: :changed
  end

  def field_change(%__MODULE__{name: :updated, changed_fields: fields}, field)
      when is_atom(field) and is_list(fields) do
    if field in fields, do: :changed, else: :unchanged
  end

  def field_change(%__MODULE__{name: :updated}, field) when is_atom(field), do: :unknown
  def field_change(%__MODULE__{}, field) when is_atom(field), do: :unchanged

  @doc """
  Returns true when an update has neither old record state nor known changed
  fields.

  These updates are handled as schema/action-wide invalidations because Upkeep
  cannot prove which field-indexed sources the record may have moved out of.
  """
  @spec broad_update?(term()) :: boolean()
  def broad_update?(%__MODULE__{name: :updated, from: nil, changed_fields: nil}), do: true

  def broad_update?(_event), do: false

  @doc """
  Returns true when an update has no old record state but does have a known set
  of changed fields.

  Partial updates can match declarative equality filters precisely for unchanged
  fields and conservatively for changed fields.
  """
  @spec partial_update?(term()) :: boolean()
  def partial_update?(%__MODULE__{name: :updated, from: nil, changed_fields: fields})
      when is_list(fields),
      do: true

  def partial_update?(_event), do: false

  def changed_fields(%__MODULE__{name: :updated, changed_fields: fields}) when is_list(fields) do
    MapSet.new(fields)
  end

  def changed_fields(%__MODULE__{name: :updated, from: from, record: record})
      when not is_nil(from) do
    [fields(record), fields(from)]
    |> Enum.flat_map(&Map.keys/1)
    |> Enum.uniq()
    |> Enum.filter(&(is_atom(&1) and field_value(from, &1) != field_value(record, &1)))
    |> MapSet.new()
  end

  def changed_fields(%__MODULE__{}), do: MapSet.new()

  @spec old(t(), field()) :: term()
  def old(%__MODULE__{from: from}, field) when is_atom(field), do: field_value(from, field)

  @spec new(t(), field()) :: term()
  def new(%__MODULE__{record: record}, field) when is_atom(field), do: field_value(record, field)

  @spec field_sets(t()) :: [map()]
  def field_sets(%__MODULE__{record: record, from: from}) do
    [record, from]
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&fields/1)
    |> Enum.uniq()
  end

  defp new(name, payload, opts) do
    record = Keyword.get(opts, :record, record_payload(payload))
    from = Keyword.get(opts, :from)

    %__MODULE__{
      name: name,
      action: Keyword.get(opts, :action),
      schema: Keyword.get(opts, :schema, schema(record || payload)),
      record: record,
      from: from,
      payload: payload,
      changed_fields: changed_fields_for(name, opts),
      meta: Keyword.get(opts, :meta, %{})
    }
  end

  defp record_payload(%_{} = payload), do: payload
  defp record_payload(payload) when is_map(payload), do: payload
  defp record_payload(_payload), do: nil

  defp schema(%schema{}), do: schema
  defp schema(_payload), do: nil

  defp fields(%_{} = struct), do: Map.from_struct(struct)
  defp fields(map) when is_map(map), do: map
  defp fields(_term), do: %{}

  defp changed_fields_for(:updated, opts),
    do: normalize_changed_fields(Keyword.get(opts, :changed_fields))

  defp changed_fields_for(_name, _opts), do: nil

  defp normalize_changed_fields(nil), do: nil

  defp normalize_changed_fields(fields) when is_list(fields) do
    fields
    |> Enum.filter(&is_atom/1)
    |> Enum.uniq()
  end

  defp field_value(nil, _field), do: nil

  defp field_value(%_{} = struct, field) do
    struct
    |> Map.from_struct()
    |> Map.get(field)
  end

  defp field_value(map, field) when is_map(map), do: Map.get(map, field)
  defp field_value(_term, _field), do: nil
end
