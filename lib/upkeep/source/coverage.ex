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

  @type entry :: map()
  @type diagnostic :: %{
          severity: :error | :warn,
          kind: :unknown | :warning | :broad,
          reason: atom(),
          label: String.t(),
          schema: module() | nil,
          schema_label: String.t() | nil,
          message: String.t(),
          action: String.t(),
          location_label: String.t() | nil,
          source_excerpt: String.t() | nil
        }
  @type t :: %__MODULE__{
          source: module(),
          params: Upkeep.Source.params(),
          precise: [entry()],
          broad: [entry()],
          explicit: [entry()],
          unknown: [entry()],
          warnings: [entry()]
        }

  @spec new(module(), map(), keyword()) :: t()
  def new(source, params, attrs \\ []) do
    struct!(__MODULE__, Keyword.merge([source: source, params: params], attrs))
  end

  @spec known?(t()) :: boolean()
  def known?(%__MODULE__{unknown: []}), do: true
  def known?(%__MODULE__{}), do: false

  @spec severity(t()) :: :ok | :warn | :error
  def severity(%__MODULE__{unknown: [_ | _]}), do: :error
  def severity(%__MODULE__{warnings: [_ | _]}), do: :warn
  def severity(%__MODULE__{}), do: :ok

  @spec summary(t()) :: String.t()
  def summary(%__MODULE__{unknown: [_ | _]}) do
    "Upkeep found gaps that can leave the source non-reactive."
  end

  def summary(%__MODULE__{precise: [_ | _], broad: [_ | _]}) do
    "Upkeep has precise keys for part of the source and broad fallbacks for the rest."
  end

  def summary(%__MODULE__{precise: [_ | _]}) do
    "Upkeep can field-index this source."
  end

  def summary(%__MODULE__{broad: [_ | _]}) do
    "Upkeep can react safely, but some changes invalidate the source broadly."
  end

  def summary(%__MODULE__{explicit: [_ | _]}) do
    "Upkeep is relying on explicit invalidation declarations."
  end

  def summary(%__MODULE__{}) do
    "Upkeep has not observed an invalidation surface for this source."
  end

  @spec diagnostics(t(), keyword()) :: [diagnostic()]
  def diagnostics(%__MODULE__{} = coverage, opts \\ []) do
    source_location = Keyword.get(opts, :source_location)

    (Enum.map(coverage.unknown, &diagnostic(:error, :unknown, &1, source_location)) ++
       Enum.map(coverage.warnings, &diagnostic(:warn, :warning, &1, source_location)) ++
       Enum.map(coverage.broad, &diagnostic(:warn, :broad, &1, source_location)))
    |> Enum.uniq_by(&{&1.kind, &1.schema, &1.reason, &1.action})
  end

  @spec explain(t(), keyword()) :: String.t()
  def explain(%__MODULE__{} = coverage, opts \\ []) do
    source = inspect(coverage.source)
    params = inspect(coverage.params)
    diagnostics = diagnostics(coverage, opts)

    details =
      diagnostics
      |> Enum.map_join("\n", fn diagnostic ->
        target =
          case diagnostic.schema_label do
            nil -> diagnostic.label
            schema -> "#{schema}: #{diagnostic.label}"
          end

        "- #{target}. #{diagnostic.message} #{diagnostic.action}"
      end)

    location = source_location_text(Keyword.get(opts, :source_location))

    [
      "Upkeep coverage for #{source} with params #{params}",
      summary(coverage),
      details,
      location
    ]
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.join("\n")
  end

  @spec merge(t(), t()) :: t()
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

  defp diagnostic(severity, kind, entry, source_location) do
    reason = Map.get(entry, :reason, :unknown)
    schema = Map.get(entry, :schema)
    {label, message, action} = reason_detail(reason)

    %{
      severity: severity,
      kind: kind,
      reason: reason,
      label: label,
      schema: schema,
      schema_label: schema_label(schema),
      message: message,
      action: action,
      location_label: location_label(source_location),
      source_excerpt: source_excerpt(source_location)
    }
  end

  defp reason_detail(:no_invalidation_surface) do
    {
      "no invalidation surface",
      "The source did not declare invalidators and its load did not report any tracked reads.",
      "Add invalidated_by/reacts_to declarations, use Upkeep.Ecto.Source for query/1 or query/2, or call Upkeep.read/1 inside an Ecto source load/1 or load/2."
    }
  end

  defp reason_detail(:fragment) do
    {
      "fragment fallback",
      "An Ecto fragment cannot be reduced to field-indexed invalidation keys safely.",
      "Keep the broad fallback, rewrite that predicate with regular Ecto comparisons, or add an explicit invalidator."
    }
  end

  defp reason_detail(:unsupported_or) do
    {
      "unsupported OR",
      "The OR expression could not be represented as precise equality or membership filters.",
      "Rewrite the OR into supported equality/in filters or add an explicit invalidator for this source."
    }
  end

  defp reason_detail(:preload) do
    {
      "preload fallback",
      "The association preload adds a broad dependency on the associated schema.",
      "Use a query preload with precise filters when this needs narrower invalidation."
    }
  end

  defp reason_detail(:many_to_many_join) do
    {
      "many-to-many join fallback",
      "The join table must be watched broadly because relationship changes can affect the source.",
      "Keep the broad fallback unless the source can declare a narrower explicit invalidator."
    }
  end

  defp reason_detail(:no_precise_filters) do
    {
      "no precise filters",
      "Upkeep can see the schema, but the query does not expose equality or membership filters.",
      "Add an equality/in filter tied to the source params or declare explicit invalidation."
    }
  end

  defp reason_detail(:unsupported_query) do
    {
      "unsupported query shape",
      "The query shape is outside Upkeep's current dependency analyzer.",
      "Declare explicit invalidation for this source or simplify the query shape."
    }
  end

  defp reason_detail(:unsupported_query_expression) do
    {
      "unsupported expression",
      "Part of the query expression could not be analyzed into invalidation keys.",
      "Rewrite that expression with supported Ecto comparisons or declare explicit invalidation."
    }
  end

  defp reason_detail(:unsupported_value_expression) do
    {
      "unsupported value expression",
      "A field comparison used a value expression Upkeep cannot turn into invalidation key values.",
      "Compare fields to source params or literal values, or declare explicit invalidation."
    }
  end

  defp reason_detail(:unknown_binding) do
    {
      "unknown binding",
      "A query predicate referenced a binding Upkeep could not map to a schema or table.",
      "Use a query shape with inspectable bindings or declare explicit invalidation."
    }
  end

  defp reason_detail(:unsupported_preload) do
    {
      "unsupported preload",
      "The preload shape cannot be inspected for reactive dependencies.",
      "Replace it with a normal association/query preload or declare explicit invalidation."
    }
  end

  defp reason_detail(:unsupported_preload_function) do
    {
      "preload function",
      "Function preloads execute opaque code that Upkeep cannot inspect.",
      "Replace the function preload with a query preload or declare explicit invalidation."
    }
  end

  defp reason_detail(:unknown_owner_binding) do
    {
      "unknown owner binding",
      "An association reference points at a query binding Upkeep could not resolve.",
      "Use a supported association join/preload shape or declare explicit invalidation."
    }
  end

  defp reason_detail(:non_schema_owner) do
    {
      "non-schema owner",
      "An association reference is owned by a table or module that is not an Ecto schema.",
      "Use schema-backed associations or declare explicit invalidation for this source."
    }
  end

  defp reason_detail(:unknown_association) do
    {
      "unknown association",
      "An association reference could not be found on the owner schema.",
      "Check the association name or declare explicit invalidation for this source."
    }
  end

  defp reason_detail(:association_lookup_failed) do
    {
      "association lookup failed",
      "Association metadata lookup raised while Upkeep was analyzing the query.",
      "Fix the schema metadata issue or declare explicit invalidation for this source."
    }
  end

  defp reason_detail(reason) do
    label = reason |> to_string() |> String.replace("_", " ")

    {
      label,
      "Upkeep preserved correctness with a non-precise invalidation path.",
      "Inspect this source and add explicit invalidation if the broad fallback is too expensive."
    }
  end

  defp source_location_text(nil), do: nil

  defp source_location_text(%{} = source_location) do
    location = location_label(source_location)
    excerpt = source_excerpt(source_location)

    cond do
      is_binary(location) and is_binary(excerpt) ->
        "Declared at #{location}\n#{excerpt}"

      is_binary(location) ->
        "Declared at #{location}"

      true ->
        nil
    end
  end

  defp location_label(nil), do: nil

  defp location_label(%{} = location) do
    file = Map.get(location, :file_label) || Map.get(location, :file)
    line = Map.get(location, :line)

    cond do
      is_binary(file) and is_integer(line) -> "#{file}:#{line}"
      is_binary(file) -> file
      is_integer(line) -> "line #{line}"
      true -> nil
    end
  end

  defp source_excerpt(nil), do: nil

  defp source_excerpt(%{} = location) do
    Map.get(location, :snippet) || Map.get(location, :code)
  end

  defp schema_label(nil), do: nil

  defp schema_label(schema) when is_atom(schema) do
    schema
    |> Module.split()
    |> Enum.join(".")
  rescue
    ArgumentError -> inspect(schema)
  end

  defp schema_label(schema), do: inspect(schema)

  defp uniq(values), do: Enum.uniq(values)
end
