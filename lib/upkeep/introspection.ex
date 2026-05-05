defmodule Upkeep.Introspection do
  @moduledoc """
  Builds a bounded, symbolic view of an Upkeep LiveView runtime.

  The introspection document deliberately describes assign shape instead of
  assign values. It is meant for development diagnostics: the real DAG comes
  from the socket runtime, while recent telemetry provides the activity trail
  around that graph.
  """

  alias Upkeep.Introspection.DeveloperView
  alias Upkeep.Live.Snapshot

  @default_event_limit 30
  @inspect_opts [limit: 6, printable_limit: 160, charlists: :as_lists]

  def snapshot(%Phoenix.LiveView.Socket{} = socket, opts \\ []) do
    event_limit = Keyword.get(opts, :event_limit, @default_event_limit)
    socket_assigns = opts |> Keyword.get(:assigns, socket.assigns) |> normalize_assigns()
    runtime = Snapshot.build(socket)
    events = recent_events(event_limit)

    assigns = decorate_assigns(runtime.assigns, socket_assigns)
    watches = decorate_watches(runtime.watches, events)
    dag = decorate_dag(runtime.dag, assigns, watches)

    %{
      live_view: live_view(socket),
      summary: DeveloperView.summary(assigns, watches, dag, events),
      dag: dag,
      assigns: assigns,
      watches: watches,
      pending_refreshes: Enum.map(runtime.pending_refreshes, &term_label/1),
      optimizations: DeveloperView.optimizations(watches, dag),
      events: events
    }
  end

  def term_label(term), do: inspect(term, @inspect_opts)

  def shape_label(shape), do: do_shape_label(shape)

  def shape(value), do: shape(value, 0)

  defp normalize_assigns(%{__struct__: Phoenix.LiveView.Socket.AssignsNotInSocket} = assigns) do
    Map.fetch!(assigns, :__assigns__)
  end

  defp normalize_assigns(assigns), do: assigns

  defp live_view(socket) do
    %{
      view: Map.get(socket, :view),
      view_label: socket |> Map.get(:view) |> module_label(),
      endpoint: Map.get(socket, :endpoint) |> module_label(),
      connected?: Phoenix.LiveView.connected?(socket)
    }
  end

  defp decorate_dag(dag, assigns, watches) do
    assigns_by_node =
      assigns
      |> Enum.group_by(& &1.node_id, & &1.assign)

    watches_by_node = Map.new(watches, &{&1.node_id, &1})

    sharing_by_node =
      assigns
      |> Enum.filter(&Map.has_key?(&1, :sharing))
      |> Map.new(&{&1.node_id, &1.sharing})

    nodes =
      dag.nodes
      |> Enum.with_index()
      |> Enum.map(fn {node, index} ->
        node_id = node.id
        assign_names = Map.get(assigns_by_node, node_id, [])
        watch = Map.get(watches_by_node, node_id)
        sharing = Map.get(sharing_by_node, node_id)

        %{
          index: index,
          id: node_id,
          dom_id: "upkeep-dag-node-#{index}",
          label: node_label(node_id),
          detail: node_detail(node_id),
          kind: node.kind,
          kind_label: Atom.to_string(node.kind),
          deps: Enum.map(node.deps, &term_label/1),
          dependents: Enum.map(node.dependents, &term_label/1),
          input_labels: Enum.map(node.deps, &DeveloperView.node_reference(&1, assigns_by_node)),
          output_labels:
            Enum.map(node.dependents, &DeveloperView.node_reference(&1, assigns_by_node)),
          assign_names: assign_names,
          watch: watch,
          sharing: sharing,
          explanation:
            DeveloperView.node_explanation(node, assign_names, watch, sharing, assigns_by_node),
          optimization: DeveloperView.node_optimization(node, watch, sharing),
          source_location:
            node_id
            |> Upkeep.Live.SourceRegistry.lookup()
            |> decorate_source_location(),
          registered_without_source:
            case Upkeep.Live.SourceRegistry.fetch(node_id) do
              {:ok, nil} -> true
              _ -> false
            end
        }
      end)

    %{
      nodes: nodes,
      edges: dag.edges,
      topological_order: Enum.map(dag.topological_order, &term_label/1)
    }
  end

  defp decorate_assigns(assigns, socket_assigns) do
    changed_assigns = Map.get(socket_assigns, :__changed__, %{})

    assigns
    |> Enum.with_index()
    |> Enum.map(fn {assign, index} ->
      value = Map.get(socket_assigns, assign.assign)

      assign
      |> Map.put(:dom_id, "upkeep-assign-#{index}")
      |> Map.put(:label, "@#{assign.assign}")
      |> Map.put(:node_label, node_label(assign.node_id))
      |> Map.merge(DeveloperView.assign_role_metadata(assign.node_id))
      |> Map.put(:shape, shape(value))
      |> Map.put(:changed?, Map.has_key?(changed_assigns, assign.assign))
    end)
  end

  defp decorate_watches(watches, events) do
    coverage_by_watch = DeveloperView.coverage_by_watch(events)

    Enum.map(watches, fn watch ->
      watch
      |> Map.put(:source_label, module_label(watch.source))
      |> Map.put(:node_label, node_label(watch.node_id))
      |> Map.put(:params_label, term_label(watch.params))
      |> Map.put(:sharing_partition_label, term_label(watch.sharing_partition))
      |> Map.put(:assign_labels, Enum.map(watch.assign_names, &"@#{&1}"))
      |> DeveloperView.decorate_watch(coverage_by_watch)
    end)
  end

  defp recent_events(limit) do
    Upkeep.recent_events(limit: limit)
    |> Enum.map(&decorate_event/1)
  catch
    :exit, _reason -> []
  end

  defp decorate_event(event) do
    %{
      name: event.event,
      name_label: event.event |> Enum.map(&to_string/1) |> Enum.join("."),
      at: event.at,
      measurements: event.measurements,
      measurements_label: term_label(event.measurements),
      metadata: event.metadata,
      metadata_label: compact_metadata(event.metadata)
    }
  end

  defp compact_metadata(metadata) when is_map(metadata) do
    metadata
    |> Enum.reject(fn {_key, value} -> is_function(value) end)
    |> Enum.take(8)
    |> Map.new()
    |> term_label()
  end

  defp compact_metadata(metadata), do: term_label(metadata)

  defp decorate_source_location(nil), do: nil

  defp decorate_source_location(location) when is_map(location) do
    code = Map.get(location, :snippet) || Map.get(location, :code)

    location
    |> Map.put(:code, code)
    |> Map.put(:location_label, location_label(location))
  end

  defp location_label(location) do
    file = Map.get(location, :file_label) || Map.get(location, :file)
    line = Map.get(location, :line)

    cond do
      is_binary(file) and is_integer(line) -> "#{file}:#{line}"
      is_binary(file) -> file
      is_integer(line) -> "line #{line}"
      true -> "source unavailable"
    end
  end

  defp node_label({:source, {source, params}}) when is_atom(source) do
    "source #{module_label(source)} #{term_label(params)}"
  end

  defp node_label({:source, source_id}), do: "source #{term_label(source_id)}"

  defp node_label({:derived, assign_name}) when is_atom(assign_name),
    do: "derived @#{assign_name}"

  defp node_label({:component, component_id}), do: "component #{term_label(component_id)}"

  defp node_label({:component_assign, component_id, assign_name}) do
    "component #{term_label(component_id)} @#{assign_name}"
  end

  defp node_label({:scope, name}) when is_atom(name), do: "scope @#{name}"
  defp node_label(node_id), do: term_label(node_id)

  defp node_detail({:source, {source, params}}) when is_atom(source) do
    "#{module_label(source)} #{term_label(params)}"
  end

  defp node_detail({:derived, assign_name}) when is_atom(assign_name), do: "@#{assign_name}"
  defp node_detail({:component, component_id}), do: term_label(component_id)

  defp node_detail({:component_assign, component_id, assign_name}),
    do: "#{term_label(component_id)} @#{assign_name}"

  defp node_detail({:scope, name}) when is_atom(name), do: "@#{name}"
  defp node_detail(node_id), do: term_label(node_id)

  defp module_label(nil), do: nil
  defp module_label(module) when is_atom(module), do: module |> Module.split() |> Enum.join(".")
  defp module_label(other), do: term_label(other)

  defp shape(_value, depth) when depth >= 3 do
    %{type: :opaque, label: "opaque"}
  end

  defp shape(nil, _depth), do: %{type: nil, label: "nil"}
  defp shape(value, _depth) when is_boolean(value), do: %{type: :boolean, label: "boolean"}
  defp shape(value, _depth) when is_integer(value), do: %{type: :integer, label: "integer"}
  defp shape(value, _depth) when is_float(value), do: %{type: :float, label: "float"}
  defp shape(value, _depth) when is_binary(value), do: %{type: :string, label: "string"}
  defp shape(value, _depth) when is_atom(value), do: %{type: :atom, label: "atom"}

  defp shape(value, depth) when is_list(value) do
    %{
      type: :list,
      label: "list",
      count: length(value),
      sample: value |> List.first() |> shape(depth + 1)
    }
  end

  defp shape(%Phoenix.HTML.Form{} = form, _depth) do
    %{
      type: :form,
      label: "form",
      name: form.name
    }
  end

  defp shape(%_struct{} = value, depth) do
    fields =
      value
      |> Map.from_struct()
      |> Map.keys()
      |> Enum.sort_by(&inspect/1)
      |> Enum.take(8)

    %{
      type: :struct,
      label: "struct",
      module: value.__struct__ |> Module.split() |> Enum.join("."),
      fields: fields,
      sample: value |> Map.from_struct() |> sample_map(depth)
    }
  end

  defp shape(value, depth) when is_map(value) do
    keys =
      value
      |> Map.keys()
      |> Enum.sort_by(&inspect/1)
      |> Enum.take(8)

    %{
      type: :map,
      label: "map",
      keys: keys,
      sample: sample_map(value, depth)
    }
  end

  defp shape(_value, _depth), do: %{type: :term, label: "term"}

  defp sample_map(map, depth) do
    map
    |> Enum.take(4)
    |> Map.new(fn {key, value} -> {key, shape(value, depth + 1)} end)
  end

  defp do_shape_label(%{type: :list, count: count, sample: sample}) do
    "list(#{count}) of #{do_shape_label(sample)}"
  end

  defp do_shape_label(%{type: :struct, module: module, fields: fields}) do
    "%#{module}{#{fields_label(fields)}}"
  end

  defp do_shape_label(%{type: :map, keys: keys}) do
    "%{#{fields_label(keys)}}"
  end

  defp do_shape_label(%{type: :form, name: name}) when is_binary(name), do: "form #{name}"
  defp do_shape_label(%{label: label}), do: label

  defp fields_label(fields) do
    fields
    |> Enum.map(&to_string/1)
    |> Enum.join(", ")
  end
end
