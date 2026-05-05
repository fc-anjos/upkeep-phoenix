defmodule Upkeep.Ecto.Source.QueryDeps.Bindings do
  @moduledoc false

  def from_query(query) do
    from_binding =
      case source_schema(query.from && query.from.source) do
        nil -> []
        schema -> [{0, schema}]
      end

    query.joins
    |> Enum.with_index(1)
    |> Enum.reduce(Map.new(from_binding), fn {join, index}, bindings ->
      schema = source_schema(join.source) || assoc_schema(bindings, join.assoc)

      if schema do
        Map.put(bindings, index, schema)
      else
        bindings
      end
    end)
  end

  def source_schema({source, nil}) when is_binary(source), do: source
  def source_schema({_source, schema}) when is_atom(schema) and not is_nil(schema), do: schema
  def source_schema(_source), do: nil

  defp assoc_schema(bindings, {owner_binding, assoc}) do
    with owner_schema when is_atom(owner_schema) <- Map.get(bindings, owner_binding),
         true <- function_exported?(owner_schema, :__schema__, 2),
         %{related: related} <- owner_schema.__schema__(:association, assoc) do
      related
    else
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp assoc_schema(_bindings, _assoc), do: nil
end
