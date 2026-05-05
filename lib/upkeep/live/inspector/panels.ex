defmodule Upkeep.Live.Inspector.Panels do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Live.Inspector.Format

  # ---------------------------------------------------------------------------
  # Overview — does anything need attention? what just happened?
  # ---------------------------------------------------------------------------

  attr :document, :map, required: true
  attr :activity, :map, required: true
  attr :timeline, :list, required: true

  def overview_panel(assigns) do
    document = assigns.document
    watches = document.watches

    reactive_gaps = Enum.filter(watches, &(&1.liveness.status == :reactive_gap))
    no_dedup = Enum.filter(watches, &(&1.dedup.status == :not_deduped))

    last_recompute =
      Enum.find(assigns.timeline, &(&1.tag == "diff"))

    last_event = List.first(assigns.timeline)
    healthy? = reactive_gaps == [] and no_dedup == []

    assigns =
      assigns
      |> assign(:reactive_gaps, reactive_gaps)
      |> assign(:no_dedup, no_dedup)
      |> assign(:last_event, last_event)
      |> assign(:last_recompute, last_recompute)
      |> assign(:healthy?, healthy?)

    ~H"""
    <div id="upkeep-overview-panel" class={panel_class()}>
      <div class={panel_inner_class()}>
        <.section_heading title="Status">
          <%= if @healthy? do %>
            <p class="m-0 max-w-3xl text-base leading-6 text-emerald-800">
              All sources live, all derived assigns shared, every query deduped.
            </p>
          <% else %>
            <p class="m-0 max-w-3xl text-base leading-6 text-amber-800">
              {format_issue_count(@reactive_gaps, "reactive gap")},
              {format_issue_count(@no_dedup, "undeduped query")}.
            </p>
          <% end %>
          <p class="mt-2 mb-0 max-w-3xl text-sm text-neutral-600">
            {@document.summary.data_sentence}
          </p>
        </.section_heading>

        <.section_heading :if={not @healthy?} title="Health" hint="things to look at">
          <ul class="m-0 list-none space-y-1.5 p-0 font-mono text-xs">
            <li :for={watch <- @reactive_gaps} class="flex flex-wrap items-baseline gap-x-2">
              <span class="text-amber-700">reactive gap</span>
              <span class="text-neutral-950">{watch.source_label}</span>
              <span class="text-neutral-500">{Enum.join(watch.assign_labels, ", ")}</span>
              <span class="text-neutral-500">— {watch.liveness.detail}</span>
            </li>
            <li :for={watch <- @no_dedup} class="flex flex-wrap items-baseline gap-x-2">
              <span class="text-amber-700">undeduped query</span>
              <span class="text-neutral-950">{watch.source_label}</span>
              <span class="text-neutral-500">— {watch.dedup.detail}</span>
            </li>
          </ul>
        </.section_heading>

        <.section_heading title="Recent activity">
          <%= cond do %>
            <% @last_recompute -> %>
              <p class="m-0 text-sm text-neutral-800">
                <span class="text-neutral-500">{@last_recompute.time_label}</span>
                — recompute pass
                <span :if={MapSet.size(@activity.recomputed) > 0} class="text-neutral-600">
                  recomputed
                  <span class="font-mono text-xs text-neutral-950">{Format.set_label(@activity.recomputed)}</span>
                </span>
                <span :if={MapSet.size(@activity.changed) > 0} class="text-neutral-600">
                  · value changed in
                  <span class="font-mono text-xs text-neutral-950">{Format.set_label(@activity.changed)}</span>
                </span>
                <span :if={MapSet.size(@activity.skipped) > 0} class="text-neutral-500">
                  · {MapSet.size(@activity.skipped)} skipped
                </span>
              </p>
            <% @last_event -> %>
              <p class="m-0 text-sm text-neutral-800">
                <span class="text-neutral-500">{@last_event.time_label}</span> —
                <span class="font-mono text-xs">{@last_event.name_label}</span>
              </p>
            <% true -> %>
              <p class="m-0 text-sm text-neutral-500">No telemetry captured yet.</p>
          <% end %>
        </.section_heading>
      </div>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Data — what assigns are on this socket and where do they come from?
  # ---------------------------------------------------------------------------

  attr :document, :map, required: true

  def data_panel(assigns) do
    ~H"""
    <div id="upkeep-playground-panel" class={panel_class()}>
      <div class={panel_inner_class()}>
        <.section_heading title="Assign Surface" hint={"#{length(@document.assigns)} keys"}>
          <%= if @document.assigns == [] do %>
            <p class="m-0 font-mono text-xs text-neutral-500">No Upkeep assigns.</p>
          <% else %>
            <table class="w-full border-collapse font-mono text-xs">
              <thead>
                <tr class="text-left">
                  <.th>Assign</.th>
                  <.th>Shape</.th>
                  <.th>Origin</.th>
                  <.th width="80">Last render</.th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={assign <- @document.assigns}
                  id={assign.dom_id}
                  class={assign_row_class(assign)}
                >
                  <.td><span class="font-semibold text-neutral-950">{assign.label}</span></.td>
                  <.td><span class="text-neutral-700">{Format.shape_label(assign.shape)}</span></.td>
                  <.td><span class="text-neutral-500">{assign.role_label}</span></.td>
                  <.td>
                    <span :if={Map.get(assign, :changed?)} class="text-emerald-700">changed</span>
                    <span :if={!Map.get(assign, :changed?)} class="text-neutral-400">unchanged</span>
                  </.td>
                </tr>
              </tbody>
            </table>
          <% end %>
        </.section_heading>
      </div>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Queries — for each source: how it's loaded, whether it dedups.
  # ---------------------------------------------------------------------------

  attr :document, :map, required: true

  def queries_panel(assigns) do
    ~H"""
    <div id="upkeep-queries-panel" class={panel_class()}>
      <div class={panel_inner_class()}>
        <.section_heading title="Sources & Queries" hint={"#{length(@document.watches)} watches"}>
          <%= if @document.watches == [] do %>
            <p class="m-0 font-mono text-xs text-neutral-500">No source watches.</p>
          <% else %>
            <div class="-mx-1 divide-y divide-neutral-200">
              <article :for={watch <- @document.watches} class="px-1 py-3 first:pt-0">
                <header class="mb-1.5 flex flex-wrap items-baseline justify-between gap-x-4 gap-y-0.5">
                  <h3 class="m-0 font-mono text-sm font-semibold text-neutral-950">
                    {watch.source_label}
                    <span class="ml-2 font-normal text-neutral-500">{Enum.join(watch.assign_labels, ", ")}</span>
                  </h3>
                  <div class="flex items-center gap-3 font-mono text-xs">
                    <span class={liveness_class(watch.liveness.status)}>{watch.liveness.label}</span>
                    <span class={dedup_class(watch.dedup.status)}>{watch.dedup.label}</span>
                  </div>
                </header>
                <dl class="m-0 grid grid-cols-[88px_minmax(0,1fr)] gap-x-4 gap-y-0.5 font-mono text-xs">
                  <.kv label="params" value={watch.params_label} />
                  <.kv label="shared by" value={watch.sharing_partition_label} />
                  <.kv
                    :if={watch.tracked_query_labels != []}
                    label="tracks"
                    value={Format.join_or_empty(watch.tracked_query_labels)}
                  />
                </dl>
                <p
                  :if={watch.liveness.status != :live_query}
                  class="mt-1.5 mb-0 text-xs leading-5 text-amber-700"
                >
                  {watch.liveness.detail}
                </p>
              </article>
            </div>
          <% end %>
        </.section_heading>
      </div>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Invalidation — for each source: will I be notified when relevant data
  # changes? Status answers that. Detail columns show what I'm watching.
  # ---------------------------------------------------------------------------

  attr :document, :map, required: true

  def invalidation_panel(assigns) do
    ~H"""
    <div id="upkeep-optimization-panel" class={panel_class()}>
      <div class={panel_inner_class()}>
        <.section_heading title="Invalidation" hint="what triggers each source to re-fetch">
          <%= if @document.watches == [] do %>
            <p class="m-0 font-mono text-xs text-neutral-500">No source watches.</p>
          <% else %>
            <table class="w-full border-collapse font-mono text-xs">
              <thead>
                <tr class="text-left">
                  <.th>Source</.th>
                  <.th width="84">Status</.th>
                  <.th>Precise</.th>
                  <.th>Broad</.th>
                  <.th>Gaps</.th>
                </tr>
              </thead>
              <tbody>
                <tr :for={watch <- @document.watches} class="align-top">
                  <.td>
                    <div class="font-semibold text-neutral-950">{watch.source_label}</div>
                    <div class="text-neutral-500">{Enum.join(watch.assign_labels, ", ")}</div>
                  </.td>
                  <.td>
                    <span class={severity_class(coverage_severity(watch.coverage))}>
                      {coverage_status(watch.coverage)}
                    </span>
                  </.td>
                  <.td>
                    <%= cond do %>
                      <% watch.coverage && watch.coverage.precise != [] -> %>
                        <span class="break-words text-neutral-900">{Format.join_or_empty(watch.coverage.precise)}</span>
                      <% !watch.coverage -> %>
                        <span class="break-words text-neutral-500">{watch.interest_keys_label}</span>
                      <% true -> %>
                        <span class="text-neutral-400">—</span>
                    <% end %>
                  </.td>
                  <.td>
                    <span :if={watch.coverage && watch.coverage.broad != []} class="break-words text-amber-700">
                      {Format.join_or_empty(watch.coverage.broad)}
                    </span>
                    <span :if={!(watch.coverage && watch.coverage.broad != [])} class="text-neutral-400">—</span>
                  </.td>
                  <.td>
                    <span :if={watch.coverage && watch.coverage.unknown != []} class="break-words text-red-700">
                      {Format.join_or_empty(watch.coverage.unknown)}
                    </span>
                    <span :if={!(watch.coverage && watch.coverage.unknown != [])} class="text-neutral-400">—</span>
                  </.td>
                </tr>
              </tbody>
            </table>

            <%= if Enum.any?(@document.watches, &(&1.coverage && &1.coverage.warnings != [])) do %>
              <h4 class="mt-5 mb-1.5 text-xs font-semibold uppercase tracking-wide text-neutral-500">
                Warnings
              </h4>
              <ul class="m-0 list-none space-y-1 p-0 font-mono text-xs">
                <li
                  :for={watch <- @document.watches}
                  :if={watch.coverage && watch.coverage.warnings != []}
                  class="flex flex-wrap items-baseline gap-x-2"
                >
                  <span class="text-neutral-950">{watch.source_label}</span>
                  <span class="text-amber-700">{Format.join_or_empty(watch.coverage.warnings)}</span>
                </li>
              </ul>
            <% end %>
          <% end %>
        </.section_heading>
      </div>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Events — what just happened? Pinned summary up top, scrolling log below.
  # ---------------------------------------------------------------------------

  attr :document, :map, required: true
  attr :activity, :map, required: true
  attr :timeline, :list, required: true

  def events_panel(assigns) do
    last_recompute_time =
      assigns.timeline
      |> Enum.find(&(&1.tag == "diff"))
      |> case do
        nil -> nil
        event -> event.time_label
      end

    assigns = assign(assigns, :last_recompute_time, last_recompute_time)

    ~H"""
    <div class="flex h-full min-h-0 flex-col bg-white">
      <div class="space-y-6 px-6 pb-5 pt-5">
        <.section_heading title="Last recompute" hint={@last_recompute_time}>
          <dl class="m-0 grid grid-cols-[110px_minmax(0,1fr)] gap-x-4 gap-y-0.5 font-mono text-xs">
            <.kv label="roots" value={Format.set_label(@activity.roots)} />
            <.kv label="recomputed" value={Format.set_label(@activity.recomputed)} />
            <.kv label="changed" value={Format.set_label(@activity.changed)} />
            <.kv label="skipped" value={Format.set_label(@activity.skipped)} />
          </dl>
        </.section_heading>

        <.section_heading
          :if={@document.pending_refreshes != []}
          title="Pending refreshes"
          hint={"#{length(@document.pending_refreshes)} queued"}
        >
          <ul class="m-0 list-none space-y-0.5 p-0 font-mono text-xs text-neutral-950">
            <li :for={refresh <- @document.pending_refreshes} class="truncate">{refresh}</li>
          </ul>
        </.section_heading>
      </div>

      <div id="upkeep-timeline-panel" class="flex min-h-0 flex-1 flex-col border-t border-neutral-200">
        <header class="flex items-baseline justify-between gap-3 border-b border-neutral-200 bg-white px-6 pb-1.5 pt-3">
          <h2 class="m-0 text-base font-semibold leading-tight text-neutral-950">Events</h2>
          <span class="font-mono text-xs text-neutral-500">{length(@timeline)} captured</span>
        </header>

        <div class="min-h-0 flex-1 overflow-auto px-6 pb-5">
          <%= if @timeline == [] do %>
            <p class="mt-3 mb-0 font-mono text-xs text-neutral-500">No telemetry events.</p>
          <% else %>
            <table class="w-full border-collapse font-mono text-xs">
              <thead>
                <tr class="text-left">
                  <.th width="80">When</.th>
                  <.th width="60">Tag</.th>
                  <.th>Event</.th>
                  <.th>Metadata</.th>
                </tr>
              </thead>
              <tbody>
                <tr
                  :for={event <- @timeline}
                  class="border-t border-neutral-100 align-baseline hover:bg-neutral-50"
                >
                  <.td><span class="text-neutral-500">{event.time_label}</span></.td>
                  <.td><span class={event_tag_class(event.tag)}>{event.tag}</span></.td>
                  <.td><span class="break-words text-neutral-950">{event.name_label}</span></.td>
                  <.td><span class="break-words text-neutral-600">{event.metadata_label}</span></.td>
                </tr>
              </tbody>
            </table>
          <% end %>
        </div>
      </div>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Source — global runtime view of the DAG. Per-node callsites live in the
  # Nodes drawer (next to the node they describe).
  # ---------------------------------------------------------------------------

  attr :layout, :map, required: true
  attr :code, :string, required: true
  attr :snapshot, :string, required: true

  def source_panel(assigns) do
    ~H"""
    <div id="upkeep-source-panel" class={panel_class()}>
      <div class={panel_inner_class()}>
        <.section_heading title="Captured Source" hint={"#{Enum.count(@layout.nodes, &Map.get(&1, :source_location))} of #{length(@layout.nodes)} nodes"}>
          <%= if not Enum.any?(@layout.nodes, &Map.get(&1, :source_location)) do %>
            <p class="m-0 text-sm text-neutral-500">
              No callsites captured. Source capture is disabled (<code class="font-mono text-xs">config :upkeep, capture_source_locations: true</code> in dev/test).
            </p>
          <% else %>
            <ul class="m-0 list-none space-y-3 p-0">
              <li :for={node <- @layout.nodes} :if={node.source_location}>
                <div class="mb-1 flex items-baseline justify-between gap-3 font-mono text-xs">
                  <a href={"##{node.inspect_dom_id}"} class="font-semibold text-neutral-950 no-underline hover:underline">
                    {node.detail}
                  </a>
                  <span class="truncate text-neutral-500">{node.source_location.location_label}</span>
                </div>
                <pre class="m-0 max-h-48 overflow-auto bg-neutral-950 px-3 py-2 font-mono text-xs leading-5 text-neutral-100"><code>{node.source_location.code}</code></pre>
              </li>
            </ul>
          <% end %>

          <%= if Enum.any?(@layout.nodes, &Map.get(&1, :registered_without_source)) do %>
            <div class="mt-4 border-t border-neutral-200 pt-3">
              <h4 class="mb-1 text-xs font-semibold uppercase tracking-wide text-amber-700">
                Registered without callsite
              </h4>
              <p class="m-0 mb-2 text-xs text-neutral-600">
                These nodes were registered by calling <code class="font-mono">Upkeep.Live.watch/derive/component</code> directly. Use the macro forms (<code class="font-mono">use Upkeep.Live</code>, then <code class="font-mono">watch/derive/component</code>) to capture file:line.
              </p>
              <ul class="m-0 list-none space-y-0.5 p-0 font-mono text-xs">
                <li :for={node <- @layout.nodes} :if={Map.get(node, :registered_without_source)}>
                  <a href={"##{node.inspect_dom_id}"} class="text-neutral-950 no-underline hover:underline">
                    {node.detail}
                  </a>
                  <span class="ml-2 text-neutral-500">{node.kind_label}</span>
                </li>
              </ul>
            </div>
          <% end %>
        </.section_heading>

        <.section_heading title="Declaration" hint="synthetic, reconstructed from the runtime DAG">
          <pre class="m-0 max-h-80 overflow-auto bg-neutral-950 px-4 py-3 font-mono text-xs leading-5 text-neutral-100"><code>{@code}</code></pre>
        </.section_heading>

        <.section_heading title="Snapshot" hint="raw runtime structure">
          <pre class="m-0 max-h-80 overflow-auto bg-neutral-950 px-4 py-3 font-mono text-xs leading-5 text-neutral-100"><code>{@snapshot}</code></pre>
        </.section_heading>
      </div>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Drawer — for each node: state, what it produces, where it's defined.
  # ---------------------------------------------------------------------------

  attr :activity, :map, required: true
  attr :layout, :map, required: true

  def node_drawer(assigns) do
    ~H"""
    <aside
      id="upkeep-node-inspector-panel"
      class="flex min-h-0 flex-col overflow-hidden bg-white max-[1100px]:max-h-[620px]"
    >
      <header class="flex items-baseline justify-between gap-3 border-b border-neutral-200 px-4 py-2.5">
        <h2 class="m-0 text-sm font-semibold text-neutral-950">Nodes</h2>
        <span class="font-mono text-xs text-neutral-500">{length(@layout.nodes)} total</span>
      </header>

      <div class="flex items-center gap-5 border-b border-neutral-200 px-4 py-2 font-mono text-xs text-neutral-500">
        <span><span class="font-semibold text-neutral-950">{MapSet.size(@activity.roots)}</span> roots</span>
        <span><span class="font-semibold text-neutral-950">{MapSet.size(@activity.changed)}</span> changed</span>
        <span><span class="font-semibold text-neutral-950">{MapSet.size(@activity.recomputed)}</span> recompute</span>
        <span><span class="font-semibold text-neutral-950">{MapSet.size(@activity.skipped)}</span> skipped</span>
      </div>

      <div class="min-h-0 flex-1 overflow-auto">
        <div
          :for={node <- @layout.nodes}
          id={node.inspect_dom_id}
          class={[
            "scroll-mt-3 border-l-2 border-transparent px-4 py-2.5",
            "border-b border-b-neutral-100 last:border-b-0",
            "target:border-l-blue-600 target:bg-blue-50/40"
          ]}
        >
          <div class="flex items-baseline justify-between gap-3">
            <a href={"##{node.dom_id}"} class="min-w-0 truncate font-mono text-xs font-semibold text-neutral-950 no-underline hover:underline">
              {node.detail}
            </a>
            <span class={["font-mono text-xs", state_text_class(node.state)]}>{node.state_label}</span>
          </div>
          <p class="mt-1 mb-0 text-xs leading-5 text-neutral-700">{node.explanation.headline}</p>

          <dl class="mt-1 grid grid-cols-[40px_minmax(0,1fr)] gap-x-3 gap-y-0.5 font-mono text-xs">
            <.kv label="in" value={Format.join_or_empty(node.explanation.inputs)} />
            <.kv label="out" value={Format.join_or_empty(node.explanation.outputs)} />
          </dl>

          <%= if node.source_location do %>
            <div class="mt-2">
              <div class="mb-1 flex items-baseline justify-between gap-2 font-mono text-xs text-neutral-500">
                <span>Captured Source</span>
                <span class="truncate text-neutral-400">{node.source_location.location_label}</span>
              </div>
              <pre class="m-0 max-h-40 overflow-auto bg-neutral-950 px-3 py-2 font-mono text-xs leading-5 text-neutral-100"><code>{node.source_location.code}</code></pre>
            </div>
          <% end %>

          <details class="mt-2">
            <summary class="cursor-pointer list-none text-xs text-neutral-500 hover:text-neutral-800">
              Details
            </summary>
            <p class="mt-1.5 mb-0 text-xs leading-5 text-neutral-700">{node.explanation.body}</p>
            <p class="mt-1 mb-0 text-xs leading-5 text-neutral-700">
              <span class="text-neutral-500">{node.optimization.label}.</span> {node.optimization.detail}
            </p>
            <ul
              :if={node.optimization.bullets != []}
              class="mt-1 mb-0 list-[square] space-y-0.5 pl-4 font-mono text-xs text-neutral-600"
            >
              <li :for={bullet <- node.optimization.bullets}>{bullet}</li>
            </ul>
            <dl class="mt-1.5 grid grid-cols-[48px_minmax(0,1fr)] gap-x-3 gap-y-0.5 font-mono text-xs">
              <.kv label="scope" value={Format.scope_label(node.scope)} />
              <.kv label="deps" value={Format.join_or_empty(node.deps)} />
            </dl>
          </details>
        </div>
      </div>
    </aside>
    """
  end

  # ---------------------------------------------------------------------------
  # Helpers.
  # ---------------------------------------------------------------------------

  attr :title, :string, required: true
  attr :hint, :any, default: nil
  slot :inner_block, required: true

  defp section_heading(assigns) do
    ~H"""
    <section class="space-y-3">
      <header class="flex items-baseline justify-between gap-3 border-b border-neutral-200 pb-1.5">
        <h2 class="m-0 text-base font-semibold leading-tight text-neutral-950">{@title}</h2>
        <span :if={@hint} class="truncate font-mono text-xs text-neutral-500">{@hint}</span>
      </header>
      {render_slot(@inner_block)}
    </section>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true

  defp kv(assigns) do
    ~H"""
    <dt class="text-neutral-500">{@label}</dt>
    <dd class="m-0 break-words text-neutral-900">{@value}</dd>
    """
  end

  attr :width, :string, default: nil
  slot :inner_block, required: true

  defp th(assigns) do
    ~H"""
    <th
      class="border-b border-neutral-200 px-2 py-1.5 text-xs font-semibold uppercase tracking-wide text-neutral-500"
      style={if(@width, do: "width: #{@width}px", else: nil)}
    >
      {render_slot(@inner_block)}
    </th>
    """
  end

  slot :inner_block, required: true

  defp td(assigns) do
    ~H"""
    <td class="px-2 py-1.5">{render_slot(@inner_block)}</td>
    """
  end

  defp panel_class, do: "h-full min-h-0 w-full overflow-auto bg-white"

  defp panel_inner_class, do: "space-y-7 px-6 py-5"

  defp assign_row_class(assign) do
    [
      "border-t border-neutral-100 align-baseline hover:bg-neutral-50",
      Map.get(assign, :changed?) && "changed bg-emerald-50"
    ]
  end

  defp state_text_class(state) when state in [:changed_root, :recompute, :changed], do: "text-blue-700"
  defp state_text_class(:skipped), do: "text-neutral-400"
  defp state_text_class(_), do: "text-neutral-500"

  defp liveness_class(:live_query), do: "text-emerald-700"
  defp liveness_class(:declared_invalidation), do: "text-emerald-700"
  defp liveness_class(:reactive_gap), do: "text-amber-700"
  defp liveness_class(_), do: "text-neutral-500"

  defp dedup_class(:read_node_cache), do: "text-emerald-700"
  defp dedup_class(:shared_and_cached), do: "text-emerald-700"
  defp dedup_class(:not_deduped), do: "text-amber-700"
  defp dedup_class(_), do: "text-neutral-500"

  defp event_tag_class("err"), do: "text-red-700"
  defp event_tag_class("diff"), do: "text-blue-700"
  defp event_tag_class(_), do: "text-neutral-500"

  defp coverage_severity(nil), do: :unknown
  defp coverage_severity(%{severity: severity}) when not is_nil(severity), do: severity
  defp coverage_severity(coverage) do
    cond do
      coverage.unknown != [] -> :error
      coverage.broad != [] -> :warn
      coverage.precise != [] -> :ok
      true -> :unknown
    end
  end

  defp coverage_status(nil), do: "no coverage"
  defp coverage_status(coverage) do
    case coverage_severity(coverage) do
      :ok -> "complete"
      :warn -> "broad"
      :error -> "gap"
      _ -> "unknown"
    end
  end

  defp severity_class(:ok), do: "text-emerald-700"
  defp severity_class(:warn), do: "text-amber-700"
  defp severity_class(:error), do: "text-red-700"
  defp severity_class(_), do: "text-neutral-500"

  defp format_issue_count([], label), do: "0 #{plural(label)}"
  defp format_issue_count([_], label), do: "1 #{label}"
  defp format_issue_count(list, label), do: "#{length(list)} #{plural(label)}"

  defp plural("undeduped query"), do: "undeduped queries"
  defp plural(label), do: label <> "s"
end
