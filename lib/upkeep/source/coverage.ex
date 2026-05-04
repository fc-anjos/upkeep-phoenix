defmodule Upkeep.Source.Coverage do
  @moduledoc """
  Describes how much of a source's invalidation surface Upkeep can see.

  `precise` entries are field-indexed. `broad` entries are known schemas or
  tables that Upkeep can only invalidate wholesale. `unknown` entries are gaps
  that may make the source non-reactive unless the app declares them explicitly.
  """

  @enforce_keys [:source, :params]
  defstruct source: nil,
            params: %{},
            precise: [],
            broad: [],
            explicit: [],
            unknown: [],
            warnings: []

  def new(source, params, attrs \\ []) do
    struct!(__MODULE__, Keyword.merge([source: source, params: params], attrs))
  end

  def known?(%__MODULE__{unknown: []}), do: true
  def known?(%__MODULE__{}), do: false

  def severity(%__MODULE__{unknown: [_ | _]}), do: :error
  def severity(%__MODULE__{warnings: [_ | _]}), do: :warn
  def severity(%__MODULE__{}), do: :ok

  def merge(%__MODULE__{} = left, %__MODULE__{} = right) do
    %__MODULE__{
      left
      | precise: uniq(left.precise ++ right.precise),
        broad: uniq(left.broad ++ right.broad),
        explicit: uniq(left.explicit ++ right.explicit),
        unknown: uniq(left.unknown ++ right.unknown),
        warnings: uniq(left.warnings ++ right.warnings)
    }
  end

  defp uniq(values), do: Enum.uniq(values)
end
