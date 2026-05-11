defmodule Upkeep.Ecto.Source.QueryDeps.Bindings do
  @moduledoc false

  defstruct by_index: %{}, diagnostics: []

  def from_query(query) do
    from_binding =
      case source_schema(query.from && query.from.source) do
        nil -> []
        schema -> [{0, schema}]
      end

    {by_index, diagnostics} =
      query.joins
      |> Enum.with_index(1)
      |> Enum.reduce({Map.new(from_binding), []}, fn {join, index}, {bindings, diagnostics} ->
        case join_schema(bindings, join) do
          {:ok, schema} ->
            {Map.put(bindings, index, schema), diagnostics}

          {:error, diagnostic} ->
            {bindings, [Map.put(diagnostic, :binding, index) | diagnostics]}

          :ignore ->
            {bindings, diagnostics}
        end
      end)

    %__MODULE__{by_index: by_index, diagnostics: Enum.reverse(diagnostics)}
  end

  def schemas(%__MODULE__{by_index: by_index}), do: by_index |> Map.values() |> MapSet.new()

  def source_schema({source, nil}) when is_binary(source), do: source
  def source_schema({_source, schema}) when is_atom(schema) and not is_nil(schema), do: schema
  def source_schema(_source), do: nil

  def association_dependencies(owner_schema, assoc) do
    with {:ok, association} <- association(owner_schema, assoc) do
      [
        {Map.get(association, :related), :preload},
        {Map.get(association, :join_through), :many_to_many_join}
      ]
      |> Enum.filter(fn {schema, _reason} -> schema?(schema) end)
      |> then(&{:ok, &1})
    end
  end

  def association_join_dependencies(owner_schema, assoc) do
    with {:ok, association} <- association(owner_schema, assoc) do
      association
      |> Map.get(:join_through)
      |> join_dependency()
    end
  end

  defp join_dependency(schema) do
    if schema?(schema), do: {:ok, [{schema, :many_to_many_join}]}, else: {:ok, []}
  end

  defp join_schema(_bindings, %{source: source}) when not is_nil(source) do
    case source_schema(source) do
      nil -> :ignore
      schema -> {:ok, schema}
    end
  end

  defp join_schema(bindings, %{assoc: {owner_binding, assoc}}) do
    with {:ok, owner_schema} <- owner_schema(bindings, owner_binding),
         {:ok, association} <- association(owner_schema, assoc) do
      {:ok, Map.get(association, :related)}
    end
  end

  defp join_schema(_bindings, _join), do: :ignore

  defp owner_schema(bindings, owner_binding) do
    case Map.fetch(bindings, owner_binding) do
      {:ok, owner_schema} -> {:ok, owner_schema}
      :error -> {:error, %{reason: :unknown_owner_binding, owner_binding: owner_binding}}
    end
  end

  defp association(owner_schema, assoc) when is_atom(owner_schema) and not is_nil(owner_schema) do
    if function_exported?(owner_schema, :__schema__, 2) do
      case owner_schema.__schema__(:association, assoc) do
        nil ->
          {:error, %{reason: :unknown_association, owner_schema: owner_schema, assoc: assoc}}

        association ->
          {:ok, association}
      end
    else
      {:error, %{reason: :non_schema_owner, owner_schema: owner_schema, assoc: assoc}}
    end
  rescue
    exception ->
      {:error,
       %{
         reason: :association_lookup_failed,
         owner_schema: owner_schema,
         assoc: assoc,
         exception: exception.__struct__
       }}
  end

  defp association(owner_schema, assoc) do
    {:error, %{reason: :non_schema_owner, owner_schema: owner_schema, assoc: assoc}}
  end

  defp schema?(schema), do: (is_atom(schema) and not is_nil(schema)) or is_binary(schema)
end
