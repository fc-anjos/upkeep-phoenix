defmodule Upkeep.Live.Inspector.Presenter do
  @moduledoc false

  def timeline_events([]), do: []

  def timeline_events(events) do
    now = System.system_time(:microsecond)

    events
    |> Enum.reverse()
    |> Enum.map(fn event ->
      event
      |> Map.put(:time_label, ago(event.at, now))
      |> Map.put(:tag, event_tag(event))
    end)
  end

  def ago(at, now) when is_integer(at) and is_integer(now) do
    delta = max(now - at, 0)
    cond do
      delta < 1_000_000 -> "just now"
      delta < 60_000_000 -> "#{div(delta, 1_000_000)}s ago"
      delta < 3_600_000_000 -> "#{div(delta, 60_000_000)}m ago"
      true -> "#{div(delta, 3_600_000_000)}h ago"
    end
  end

  def ago(_at, _now), do: ""

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
