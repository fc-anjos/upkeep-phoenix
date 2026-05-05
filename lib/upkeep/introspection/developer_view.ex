defmodule Upkeep.Introspection.DeveloperView do
  @moduledoc false

  @inspect_opts [limit: 6, printable_limit: 160, charlists: :as_lists]

  def summary(assigns, watches, dag, events) do
    loaded = Enum.count(assigns, &(&1.role == :loaded))
    computed = Enum.count(assigns, &(&1.role == :computed))
    context = Enum.count(assigns, &(&1.role == :context))
    live_queries = Enum.count(watches, &(&1.liveness.status == :live_query))
    reactive_gaps = Enum.count(watches, &(&1.liveness.status == :reactive_gap))

    local_derives =
      Enum.count(dag.nodes, &(&1.kind == :derived and &1.optimization.status == :local))

    shared_derives =
      Enum.count(dag.nodes, &(&1.kind == :derived and &1.optimization.status == :shared))

    %{
      data_sentence:
        "#{pluralize(length(assigns), "assign")} on this page: #{loaded} loaded, #{computed} computed, #{context} context.",
      live_sentence:
        "#{pluralize(live_queries, "live query")} tracked; #{pluralize(reactive_gaps, "reactive gap")} found.",
      compute_sentence:
        "#{pluralize(shared_derives, "shared computation")} and #{pluralize(local_derives, "local computation")}.",
      counts: %{
        assigns: length(assigns),
        sources: length(watches),
        nodes: length(dag.nodes),
        edges: length(dag.edges),
        events: length(events)
      }
    }
  end

  def assign_role_metadata(node_id) do
    role = assign_role(node_id)

    %{
      role: role,
      role_label: role_label(role),
      role_detail: assign_role_detail(node_id)
    }
  end

  def decorate_watch(watch, coverage_by_watch) do
    coverage =
      Map.get(coverage_by_watch, {watch.source, watch.params}) || coverage_from_watch(watch)

    tracked_deps = Map.get(watch, :tracked_deps, [])

    watch
    |> Map.put(:tracked_query_count, length(tracked_deps))
    |> Map.put(:tracked_query_labels, Enum.map(tracked_deps, &query_dep_label/1))
    |> Map.put(:interest_key_count, length(watch.interest_keys))
    |> Map.put(
      :interest_keys_label,
      watch.interest_keys |> Enum.map(&term_label/1) |> join_or_empty()
    )
    |> Map.put(:coverage, coverage)
    |> Map.put(:liveness, source_liveness(watch, coverage))
    |> Map.put(:dedup, source_dedup(watch, tracked_deps))
  end

  def coverage_by_watch(events) do
    events
    |> Enum.filter(&(&1.name == [:upkeep, :source, :coverage]))
    |> Map.new(fn event ->
      metadata = event.metadata
      key = {metadata.source, metadata.params}
      {key, decorate_coverage(metadata)}
    end)
  end

  def node_explanation(
        %{id: {:scope, _name}} = node,
        assign_names,
        _watch,
        _sharing,
        assigns_by_node
      ) do
    outputs = Enum.map(node.dependents, &node_reference(&1, assigns_by_node))

    %{
      headline: "#{assign_names_label(assign_names)} is LiveView/session context.",
      body:
        "It is intentionally local to this socket and can make downstream computations user-specific.",
      inputs: [],
      outputs: outputs
    }
  end

  def node_explanation(%{kind: :source} = node, assign_names, watch, _sharing, assigns_by_node) do
    assigns = assign_names_label(assign_names)
    outputs = Enum.map(node.dependents, &node_reference(&1, assigns_by_node))

    %{
      headline: "#{assigns} comes from #{watch.source_label}.",
      body:
        "It loads with #{watch.params_label} and feeds #{if(outputs == [], do: "the rendered assign surface", else: join_or_empty(outputs))}.",
      inputs: ["params #{watch.params_label}"],
      outputs: Enum.uniq(watch.assign_labels ++ outputs)
    }
  end

  def node_explanation(%{kind: :derived} = node, assign_names, _watch, sharing, assigns_by_node) do
    inputs = Enum.map(node.deps, &node_reference(&1, assigns_by_node))
    outputs = Enum.map(node.dependents, &node_reference(&1, assigns_by_node))
    assigns = assign_names_label(assign_names)

    %{
      headline: "#{assigns} is computed from #{join_or_empty(inputs)}.",
      body: sharing_sentence(sharing),
      inputs: inputs,
      outputs: outputs
    }
  end

  def node_explanation(node, assign_names, _watch, _sharing, assigns_by_node) do
    inputs = Enum.map(node.deps, &node_reference(&1, assigns_by_node))
    outputs = Enum.map(node.dependents, &node_reference(&1, assigns_by_node))

    %{
      headline: "#{assign_names_label(assign_names)} is a #{node.kind} node.",
      body: "It participates in the current LiveView dependency graph.",
      inputs: inputs,
      outputs: outputs
    }
  end

  def node_optimization(%{id: {:scope, _name}}, _watch, _sharing), do: scope_node_optimization()
  def node_optimization(%{kind: :source}, watch, _sharing), do: source_node_optimization(watch)
  def node_optimization(%{kind: :derived}, _watch, sharing), do: derive_node_optimization(sharing)
  def node_optimization(_node, _watch, _sharing), do: local_node_optimization()

  def optimizations(watches, dag) do
    source_items =
      Enum.map(watches, fn watch ->
        %{
          label: assign_names_label(watch.assign_names),
          kind: :source,
          status: watch.liveness.status,
          status_label: watch.liveness.label,
          detail: watch.liveness.detail,
          dedup_label: watch.dedup.label,
          dedup_detail: watch.dedup.detail,
          coverage: watch.coverage,
          source_label: watch.source_label
        }
      end)

    derive_items =
      dag.nodes
      |> Enum.filter(&(&1.kind == :derived))
      |> Enum.map(fn node ->
        %{
          label: assign_names_label(node.assign_names),
          kind: :derived,
          status: node.optimization.status,
          status_label: node.optimization.label,
          detail: node.optimization.detail,
          dedup_label: nil,
          dedup_detail: nil,
          coverage: nil,
          source_label: nil
        }
      end)

    source_items ++ derive_items
  end

  defp decorate_coverage(%{coverage: %Upkeep.Source.Coverage{} = coverage} = metadata) do
    %{
      severity: Map.get(metadata, :severity),
      severity_label: coverage_severity_label(Map.get(metadata, :severity)),
      known?: Map.get(metadata, :known?),
      precise: Enum.map(coverage.precise, &coverage_entry_label/1),
      broad: Enum.map(coverage.broad, &coverage_entry_label/1),
      explicit: Enum.map(coverage.explicit, &term_label/1),
      unknown: Enum.map(coverage.unknown, &coverage_gap_label/1),
      warnings: Enum.map(coverage.warnings, &coverage_gap_label/1),
      summary: coverage_summary(coverage)
    }
  end

  defp decorate_coverage(_metadata), do: nil

  defp coverage_from_watch(watch) do
    explicit =
      if function_exported?(watch.source, :__upkeep_explicit_interest_keys__, 1),
        do: watch.source.__upkeep_explicit_interest_keys__(watch.params),
        else: []

    coverage =
      watch
      |> Map.get(:tracked_deps, [])
      |> Enum.reduce(
        Upkeep.Source.Coverage.new(watch.source, watch.params, explicit: explicit),
        fn deps, coverage ->
          Upkeep.Source.Coverage.merge(coverage, Upkeep.Ecto.QueryDeps.coverage(deps))
        end
      )
      |> attach_unknown_if_empty()

    decorate_coverage(%{
      coverage: coverage,
      severity: Upkeep.Source.Coverage.severity(coverage),
      known?: Upkeep.Source.Coverage.known?(coverage)
    })
  rescue
    _error -> nil
  end

  defp attach_unknown_if_empty(%Upkeep.Source.Coverage{} = coverage) do
    empty? =
      coverage.precise == [] and coverage.broad == [] and coverage.explicit == [] and
        coverage.unknown == []

    if empty? do
      %Upkeep.Source.Coverage{
        coverage
        | unknown: [%{reason: :no_invalidation_surface}]
      }
    else
      coverage
    end
  end

  defp assign_role({:source, _source_id}), do: :loaded
  defp assign_role({:derived, _assign_name}), do: :computed
  defp assign_role({:scope, _name}), do: :context
  defp assign_role({:component, _component_id}), do: :component
  defp assign_role({:component_assign, _component_id, _assign_name}), do: :component
  defp assign_role(_node_id), do: :runtime

  defp assign_role_detail({:source, _source_id}), do: "Loaded by an Upkeep source."
  defp assign_role_detail({:derived, _assign_name}), do: "Computed from other assigns."
  defp assign_role_detail({:scope, _name}), do: "Current LiveView/session context."
  defp assign_role_detail({:component, _component_id}), do: "Component-local runtime state."

  defp assign_role_detail({:component_assign, _component_id, _assign_name}),
    do: "Extracted from component state."

  defp assign_role_detail(_node_id), do: "Runtime-managed value."

  defp role_label(:loaded), do: "loaded"
  defp role_label(:computed), do: "computed"
  defp role_label(:context), do: "context"
  defp role_label(:component), do: "component"
  defp role_label(:runtime), do: "runtime"

  defp source_liveness(watch, coverage) do
    tracked_count = length(Map.get(watch, :tracked_deps, []))
    key_count = length(watch.interest_keys)
    registered? = Map.get(watch, :registered?, false)
    coverage_severity = if coverage, do: coverage.severity

    cond do
      not registered? ->
        %{
          status: :not_registered,
          label: "not subscribed",
          detail: "This source loaded, but this LiveView is not connected to the shared graph."
        }

      key_count == 0 or coverage_severity == :error ->
        %{
          status: :reactive_gap,
          label: "not fully live",
          detail: "Upkeep cannot prove a complete invalidation surface for this source."
        }

      tracked_count > 0 ->
        %{
          status: :live_query,
          label: "live query",
          detail:
            "Upkeep.read tracked #{pluralize(tracked_count, "query")} for invalidation and read-node reuse."
        }

      true ->
        %{
          status: :declared_invalidation,
          label: "declared live",
          detail:
            "This source reacts through explicit invalidation keys, but no Ecto query was tracked."
        }
    end
  end

  defp source_dedup(watch, tracked_deps) do
    assign_count = length(watch.assign_names)
    tracked_count = length(tracked_deps)

    cond do
      assign_count > 1 and tracked_count > 0 ->
        %{
          status: :shared_and_cached,
          label: "shared load + read cache",
          detail:
            "#{pluralize(assign_count, "assign")} share this source node, and #{pluralize(tracked_count, "query")} can reuse read-node cache."
        }

      assign_count > 1 ->
        %{
          status: :shared_source,
          label: "shared source",
          detail: "#{pluralize(assign_count, "assign")} are backed by one source node."
        }

      tracked_count > 0 ->
        %{
          status: :read_node_cache,
          label: "read-node cache",
          detail:
            "#{pluralize(tracked_count, "query")} can be coalesced and reused across matching loads."
        }

      true ->
        %{
          status: :not_deduped,
          label: "no query dedup",
          detail: "No Upkeep.read query was tracked, so read-node caching cannot help this load."
        }
    end
  end

  defp source_node_optimization(nil), do: local_node_optimization()

  defp source_node_optimization(watch) do
    %{
      status: watch.liveness.status,
      label: watch.liveness.label,
      detail: watch.liveness.detail,
      bullets:
        [
          "Invalidation keys: #{watch.interest_key_count}",
          "Tracked queries: #{watch.tracked_query_count}",
          "Dedup: #{watch.dedup.detail}"
        ] ++ coverage_bullets(watch.coverage)
    }
  end

  defp derive_node_optimization(%{result: :shared} = sharing) do
    %{
      status: :shared,
      label: "shared compute",
      detail: "This derived assign is registered in the shared graph and can reuse compute work.",
      bullets: [
        "Why: #{sharing_reason_label(Map.get(sharing, :reason))}",
        "Partition: #{term_label(Map.get(sharing, :sharing_partition))}"
      ]
    }
  end

  defp derive_node_optimization(%{result: :local} = sharing) do
    %{
      status: :local,
      label: "local compute",
      detail: "This derived assign stays on this LiveView socket.",
      bullets:
        [
          "Why: #{sharing_reason_label(Map.get(sharing, :reason))}"
        ] ++ sharing_boundary_bullets(sharing)
    }
  end

  defp derive_node_optimization(_sharing) do
    %{
      status: :local,
      label: "local compute",
      detail: "No sharing diagnostics were captured for this node.",
      bullets: []
    }
  end

  defp scope_node_optimization do
    %{
      status: :local_context,
      label: "local context",
      detail: "Scope data is per socket, so it intentionally blocks global sharing downstream.",
      bullets: ["Depends on the current LiveView session/user."]
    }
  end

  defp local_node_optimization do
    %{
      status: :local,
      label: "local",
      detail: "This node is evaluated inside the current LiveView.",
      bullets: []
    }
  end

  def node_reference(node_id, assigns_by_node) do
    case Map.get(assigns_by_node, node_id, []) do
      [] -> node_detail(node_id)
      assign_names -> assign_names_label(assign_names)
    end
  end

  defp node_detail({:source, {source, params}}) when is_atom(source) do
    "#{module_label(source)} #{term_label(params)}"
  end

  defp node_detail({:derived, assign_name}) when is_atom(assign_name), do: "@#{assign_name}"
  defp node_detail({:component, component_id}), do: term_label(component_id)

  defp node_detail({:component_assign, component_id, assign_name}),
    do: "#{term_label(component_id)} @#{assign_name}"

  defp node_detail({:scope, name}) when is_atom(name), do: "@#{name}"
  defp node_detail(node_id), do: term_label(node_id)

  defp assign_names_label([]), do: "this node"

  defp assign_names_label(assign_names) do
    assign_names
    |> Enum.map(&"@#{&1}")
    |> Enum.join(", ")
  end

  defp coverage_severity_label(:ok), do: "complete"
  defp coverage_severity_label(:warn), do: "broad"
  defp coverage_severity_label(:error), do: "gap"
  defp coverage_severity_label(_severity), do: "unknown"

  defp coverage_entry_label(%{schema: schema, fields: fields}) when is_list(fields) do
    "#{module_label(schema)} on #{Enum.map_join(fields, ", ", &to_string/1)}"
  end

  defp coverage_entry_label(%{schema: schema, reason: reason}) do
    "#{module_label(schema)} (#{coverage_gap_label(%{reason: reason})})"
  end

  defp coverage_entry_label(entry), do: term_label(entry)

  defp coverage_gap_label(%{reason: :unsupported_query}), do: "unsupported query shape"
  defp coverage_gap_label(%{reason: :no_precise_filters}), do: "no precise filters"
  defp coverage_gap_label(%{reason: :unsupported_query_expression}), do: "unsupported expression"
  defp coverage_gap_label(%{reason: :unsupported_preload}), do: "unsupported preload"
  defp coverage_gap_label(%{reason: :unsupported_preload_function}), do: "preload function"
  defp coverage_gap_label(%{reason: :no_invalidation_surface}), do: "no invalidation surface"
  defp coverage_gap_label(%{reason: reason}), do: to_string(reason)
  defp coverage_gap_label(gap), do: term_label(gap)

  defp coverage_summary(%Upkeep.Source.Coverage{unknown: [_ | _]}) do
    "Upkeep found an invalidation gap."
  end

  defp coverage_summary(%Upkeep.Source.Coverage{precise: [_ | _], broad: [_ | _]}) do
    "Precise invalidation is available; some dependencies invalidate broadly."
  end

  defp coverage_summary(%Upkeep.Source.Coverage{precise: [_ | _]}) do
    "Precise invalidation is available."
  end

  defp coverage_summary(%Upkeep.Source.Coverage{broad: [_ | _]}) do
    "Invalidation is broad but known."
  end

  defp coverage_summary(%Upkeep.Source.Coverage{explicit: [_ | _]}) do
    "Invalidation comes from explicit declarations."
  end

  defp coverage_summary(%Upkeep.Source.Coverage{}) do
    "No invalidation surface was observed."
  end

  defp coverage_bullets(nil), do: []

  defp coverage_bullets(coverage) do
    [
      "Coverage: #{coverage.summary}",
      coverage.precise != [] && "Precise: #{join_or_empty(coverage.precise)}",
      coverage.broad != [] && "Broad: #{join_or_empty(coverage.broad)}",
      coverage.unknown != [] && "Gaps: #{join_or_empty(coverage.unknown)}",
      coverage.warnings != [] && "Warnings: #{join_or_empty(coverage.warnings)}"
    ]
    |> Enum.reject(&(&1 in [nil, false]))
  end

  defp query_dep_label(%Upkeep.Ecto.QueryDeps{} = deps) do
    schemas =
      deps.schemas
      |> MapSet.to_list()
      |> Enum.map(&module_label/1)
      |> join_or_empty()

    filters =
      deps.equality_filters
      |> Enum.flat_map(fn {schema, filters} ->
        Enum.map(filters, fn {field, values} ->
          "#{module_label(schema)}.#{field}=#{term_label(values)}"
        end)
      end)

    mode =
      cond do
        deps.broad? -> "broad"
        filters == [] -> "broad"
        true -> "precise"
      end

    "#{mode} query on #{schemas}#{if(filters == [], do: "", else: " filtered by #{Enum.join(filters, ", ")}")}"
  end

  defp query_dep_label(dep), do: term_label(dep)

  defp sharing_sentence(%{result: :shared}) do
    "Upkeep can share this computation because every dependency is shareable and the function is externally identifiable."
  end

  defp sharing_sentence(%{result: :local, reason: reason}) do
    "This computation stays local because #{sharing_reason_label(reason)}."
  end

  defp sharing_sentence(_sharing), do: "No sharing decision was captured for this computation."

  defp sharing_reason_label(:shareable), do: "the dependencies and function are shareable"

  defp sharing_reason_label(:local_fun),
    do: "the function is local/anonymous and cannot be globally identified"

  defp sharing_reason_label(:captured_scope), do: "the function captured socket or current scope"
  defp sharing_reason_label(:captured_fun), do: "the function closes over local values"
  defp sharing_reason_label(:disconnected_socket), do: "the LiveView was not connected"
  defp sharing_reason_label(:component_scoped_dep), do: "a dependency is scoped to a component"
  defp sharing_reason_label(:local_only_dep), do: "an upstream dependency is already local-only"
  defp sharing_reason_label(:unsupported_dep), do: "a dependency type is not shareable"

  defp sharing_reason_label(:cross_partition_dep),
    do: "dependencies belong to different sharing partitions"

  defp sharing_reason_label(:empty_deps), do: "there are no shareable dependencies"
  defp sharing_reason_label(:current_scope), do: "it depends on current_scope"
  defp sharing_reason_label(:component_boundary), do: "it crosses a component boundary"
  defp sharing_reason_label(:error), do: "sharing analysis failed"
  defp sharing_reason_label(nil), do: "no reason was recorded"
  defp sharing_reason_label(reason), do: to_string(reason)

  defp sharing_boundary_bullets(sharing) do
    sharing
    |> Map.get(:shareable_plan, %{})
    |> Map.get(:boundaries, [])
    |> Enum.map(fn boundary ->
      "Boundary: #{node_detail(boundary.node_id)} (#{sharing_reason_label(boundary.reason)})"
    end)
  end

  defp join_or_empty([]), do: "empty"
  defp join_or_empty(values), do: Enum.join(values, ", ")

  defp pluralize(1, "query"), do: "1 query"
  defp pluralize(count, "query"), do: "#{count} queries"
  defp pluralize(1, "assign"), do: "1 assign"
  defp pluralize(count, "assign"), do: "#{count} assigns"
  defp pluralize(1, "live query"), do: "1 live query"
  defp pluralize(count, "live query"), do: "#{count} live queries"
  defp pluralize(1, "reactive gap"), do: "1 reactive gap"
  defp pluralize(count, "reactive gap"), do: "#{count} reactive gaps"
  defp pluralize(1, "shared computation"), do: "1 shared computation"
  defp pluralize(count, "shared computation"), do: "#{count} shared computations"
  defp pluralize(1, "local computation"), do: "1 local computation"
  defp pluralize(count, "local computation"), do: "#{count} local computations"

  defp module_label(nil), do: nil
  defp module_label(module) when is_atom(module), do: module |> Module.split() |> Enum.join(".")
  defp module_label(other), do: term_label(other)

  defp term_label(term), do: inspect(term, @inspect_opts)
end
