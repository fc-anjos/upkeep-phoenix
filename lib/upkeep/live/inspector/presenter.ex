defmodule Upkeep.Live.Inspector.Presenter do
  @moduledoc false

  def timeline_events([]), do: []

  def timeline_events(events) do
    base =
      events
      |> List.first()
      |> Map.get(:at)

    events
    |> Enum.reverse()
    |> Enum.map(fn event ->
      event
      |> Map.put(:time_label, relative_time(event.at, base))
      |> Map.put(:tag, event_tag(event))
    end)
  end

  def generated_declaration(document) do
    source_lines =
      Enum.flat_map(document.watches, fn watch ->
        Enum.map(watch.assign_names, fn assign_name ->
          "|> Upkeep.watch(:#{assign_name}, #{watch.source_label}, #{watch.params_label})"
        end)
      end)

    derived_lines =
      document.dag.nodes
      |> Enum.filter(&(&1.kind == :derived))
      |> Enum.flat_map(fn node ->
        Enum.map(node.assign_names, fn assign_name ->
          deps =
            document.dag.edges
            |> Enum.filter(&(&1.to == node.id))
            |> Enum.map(&assign_name_for(document.dag.nodes, &1.from))
            |> Enum.reject(&is_nil/1)
            |> Enum.map(&":#{&1}")
            |> Enum.join(", ")

          "|> Upkeep.derive(:#{assign_name}, [#{deps}], &.../1)"
        end)
      end)

    component_lines =
      document.dag.nodes
      |> Enum.filter(&(&1.kind == :component))
      |> Enum.map(fn node -> "# component #{node.detail}" end)

    (["# Generated from current runtime DAG", "socket"] ++
       source_lines ++ derived_lines ++ component_lines)
    |> Enum.join("\n")
  end

  def graph_snapshot(document) do
    order =
      document.dag.topological_order
      |> Enum.map(&"  #{&1}")
      |> Enum.join(",\n")

    """
    graph_snapshot(%{
      live_view: #{inspect(document.live_view.view_label)},
      nodes: #{length(document.dag.nodes)},
      edges: #{length(document.dag.edges)},
      topological_order: [
    #{order}
      ]
    })
    """
    |> String.trim()
  end

  defp relative_time(at, base) when is_integer(at) and is_integer(base) do
    seconds = max(at - base, 0) / 1_000_000
    "+" <> :erlang.float_to_binary(seconds, decimals: 2) <> "s"
  end

  defp relative_time(_at, _base), do: "-"

  defp event_tag(%{name: [:upkeep, :dag, :recompute | _]}), do: "diff"

  defp event_tag(%{name: name}) when is_list(name) do
    if Enum.any?(name, &(&1 == :exception)), do: "err", else: "tel"
  end

  defp assign_name_for(nodes, node_id) do
    nodes
    |> Enum.find(&(&1.id == node_id))
    |> case do
      %{assign_names: [assign_name | _]} -> assign_name
      _ -> nil
    end
  end
end
