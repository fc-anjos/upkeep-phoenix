defmodule Upkeep.Live.Inspector.Layout do
  @moduledoc false

  alias Upkeep.Introspection
  alias Upkeep.Live.Inspector.Format

  @accent "#2563eb"
  @neutral_border "#d4d4d4"

  def build(dag, activity) do
    node_width = 176
    node_height = 64
    gap_x = 34
    gap_y = 58
    padding = 44

    layer_by_id = layers_for(dag.nodes, dag.edges)

    layers =
      dag.nodes
      |> Enum.group_by(&Map.fetch!(layer_by_id, &1.id))
      |> Enum.sort_by(fn {layer, _nodes} -> layer end)
      |> Enum.map(fn {_layer, nodes} -> Enum.sort_by(nodes, & &1.index) end)

    max_layer_size = layers |> Enum.map(&length/1) |> Enum.max(fn -> 1 end)
    cell_width = node_width + gap_x
    cell_height = node_height + gap_y

    nodes =
      layers
      |> Enum.with_index()
      |> Enum.flat_map(fn {layer_nodes, layer_index} ->
        total_width = length(layer_nodes) * cell_width - gap_x
        start_x = padding + (max_layer_size * cell_width - total_width) / 2

        layer_nodes
        |> Enum.with_index()
        |> Enum.map(fn {node, index} ->
          node
          |> Map.put(:x, round(start_x + index * cell_width))
          |> Map.put(:y, padding + layer_index * cell_height)
          |> Map.put(:width, node_width)
          |> Map.put(:height, node_height)
          |> decorate_node(activity)
        end)
      end)

    %{
      nodes: nodes,
      edges: layout_edges(dag.edges, nodes, activity),
      width: padding * 2 + max_layer_size * cell_width - gap_x,
      height: padding * 2 + length(layers) * cell_height - gap_y
    }
  end

  defp layout_edges(edges, nodes, activity) do
    by_id = Map.new(nodes, &{&1.id, &1})

    Enum.flat_map(edges, fn edge ->
      with %{from: from_id, to: to_id} <- edge,
           {:ok, from} <- Map.fetch(by_id, from_id),
           {:ok, to} <- Map.fetch(by_id, to_id) do
        active? =
          MapSet.member?(activity.roots, from_id) or
            MapSet.member?(activity.recomputed, from_id) or
            MapSet.member?(activity.changed, to_id) or
            MapSet.member?(activity.recomputed, to_id)

        [
          %{
            path: edge_path(from, to),
            active?: active?,
            stroke: if(active?, do: @accent, else: "#bdbdbd"),
            width: if(active?, do: 1.5, else: 1.0)
          }
        ]
      else
        _ -> []
      end
    end)
  end

  defp layers_for(nodes, edges) do
    Enum.reduce(nodes, %{}, fn node, layers ->
      parents =
        edges
        |> Enum.filter(&(&1.to == node.id))
        |> Enum.map(& &1.from)

      layer =
        parents
        |> Enum.map(&(Map.get(layers, &1, 0) + 1))
        |> Enum.max(fn -> 0 end)

      Map.put(layers, node.id, layer)
    end)
  end

  defp decorate_node(node, activity) do
    state = node_state(node.id, activity)
    scope = Map.get(activity.scopes, node.id, :unknown)
    active? = Format.state_active?(state)

    node
    |> Map.put(:inspect_dom_id, "#{node.dom_id}-inspect")
    |> Map.put(:id_label, Introspection.term_label(node.id))
    |> Map.put(:short_label, short_label(node))
    |> Map.put(:kind_code, kind_code(node.kind))
    |> Map.put(:scope, scope)
    |> Map.put(:state, state)
    |> Map.put(:state_label, Format.state_label(state))
    |> Map.put(:fill, "#ffffff")
    |> Map.put(:stroke, if(active?, do: @accent, else: @neutral_border))
    |> maybe_put_step(Map.get(activity.steps, node.id))
  end

  defp maybe_put_step(node, nil), do: node
  defp maybe_put_step(node, step), do: Map.put(node, :step, step)

  defp node_state(node_id, activity) do
    cond do
      MapSet.member?(activity.roots, node_id) -> :changed_root
      MapSet.member?(activity.changed, node_id) -> :changed
      MapSet.member?(activity.recomputed, node_id) -> :recompute
      MapSet.member?(activity.skipped, node_id) -> :skipped
      is_nil(activity.latest_recompute) -> :cold
      true -> :idle
    end
  end

  defp edge_path(from, to) do
    x1 = from.x + from.width / 2
    y1 = from.y + from.height
    x2 = to.x + to.width / 2
    y2 = to.y
    dy = y2 - y1

    "M #{x1},#{y1} C #{x1},#{y1 + dy * 0.5} #{x2},#{y2 - dy * 0.5} #{x2},#{y2}"
  end

  defp kind_code(:source), do: "src"
  defp kind_code(:derived), do: "der"
  defp kind_code(:component), do: "cmp"
  defp kind_code(_kind), do: "node"

  defp short_label(%{kind: :source, assign_names: [assign | _]}), do: "@#{assign}"
  defp short_label(%{assign_names: [assign | _]}), do: "@#{assign}"
  defp short_label(%{detail: detail}), do: detail
end
