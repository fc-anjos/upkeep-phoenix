defmodule Upkeep.Source.QueryDeps.Expressions do
  @moduledoc false

  @unsupported_nodes [:or, :fragment]

  def all(query) do
    query
    |> structs()
    |> Enum.map(& &1.expr)
  end

  def structs(query) do
    [
      Enum.map(query.joins, & &1.on),
      query.wheres,
      query.order_bys,
      query.group_bys,
      query.havings,
      List.wrap(query.select)
    ]
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
  end

  def field_ref({{:., _dot_meta, [{:&, _binding_meta, [binding]}, field]}, _meta, []})
      when is_integer(binding) and is_atom(field) do
    {binding, field}
  end

  def field_ref(_node), do: nil

  def unsupported?(expr) do
    {_expr, unsupported?} =
      Macro.prewalk(expr, false, fn
        {node, _meta, _args} = expr, _unsupported? when node in @unsupported_nodes ->
          {expr, true}

        expr, unsupported? ->
          {expr, unsupported?}
      end)

    unsupported?
  end
end
