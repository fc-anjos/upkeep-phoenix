defmodule Upkeep.Live.Inspector.Panels do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Live.Inspector.Format

  attr :document, :map, required: true
  attr :activity, :map, required: true

  def overview_panel(assigns) do
    ~H"""
    <div id="upkeep-overview-panel" class={scroll_panel_class()}>
      <div class="grid gap-3 p-4">
        <div class="grid grid-cols-[minmax(0,1fr)_340px] gap-3 max-[1100px]:grid-cols-1">
          <section class={card_class()}>
            <.section_header title="Sources" meta={"#{length(@document.watches)} watches"} />
            <div class="divide-y divide-neutral-200">
              <div
                :for={watch <- @document.watches}
                class="grid grid-cols-[minmax(0,1fr)_auto_auto] items-center gap-3 px-3 py-2"
              >
                <div class="min-w-0">
                  <div class="truncate font-mono text-xs font-semibold text-neutral-950">
                    {watch.source_label}
                  </div>
                  <div class="truncate font-mono text-xs text-neutral-500">
                    {Format.join_or_empty(watch.assign_labels)}
                  </div>
                </div>
                <.chip label={watch.liveness.label} />
                <.chip label={watch.dedup.label} />
              </div>
              <.empty_message :if={@document.watches == []} text="No source watches." />
            </div>
          </section>

          <section class={card_class()}>
            <.section_header title="Compute" meta={@document.summary.compute_sentence} />
            <div class="divide-y divide-neutral-200">
              <div
                :for={item <- @document.optimizations}
                :if={item.kind == :derived}
                class="px-3 py-2"
              >
                <div class="flex items-center justify-between gap-3">
                  <span class="truncate font-mono text-xs font-semibold text-neutral-950">
                    {item.label}
                  </span>
                  <.chip label={item.status_label} />
                </div>
                <p class="mt-1 mb-0 text-xs leading-5 text-neutral-600">{item.detail}</p>
              </div>
              <.empty_message
                :if={not Enum.any?(@document.optimizations, &(&1.kind == :derived))}
                text="No derived assigns."
              />
            </div>
          </section>
        </div>

        <section class={card_class()}>
          <.section_header title="Queue" meta={"#{length(@document.pending_refreshes)} pending"} />
          <div class="divide-y divide-neutral-200 font-mono text-xs">
            <div
              :for={refresh <- @document.pending_refreshes}
              class="grid grid-cols-[96px_minmax(0,1fr)] gap-3 px-3 py-2"
            >
              <span class="text-neutral-500">root</span>
              <span class="truncate text-neutral-950">{refresh}</span>
            </div>
            <.empty_message :if={@document.pending_refreshes == []} text="Nothing queued." />
          </div>
        </section>
      </div>
    </div>
    """
  end

  attr :document, :map, required: true

  def data_panel(assigns) do
    ~H"""
    <div id="upkeep-playground-panel" class={scroll_panel_class()}>
      <div class="p-4">
        <section class={card_class()}>
          <.section_header title="Assign Surface" meta={"#{length(@document.assigns)} keys"} />
          <div class="divide-y divide-neutral-200 font-mono text-xs">
            <div
              :for={assign <- @document.assigns}
              id={assign.dom_id}
              class={assign_row_class(assign)}
            >
              <span class="min-w-0 truncate font-semibold text-neutral-950">{assign.label}</span>
              <span class="min-w-0 truncate text-neutral-700">
                {Format.shape_label(assign.shape)}
              </span>
              <.chip label={assign.role_label} />
            </div>
            <.empty_message :if={@document.assigns == []} text="No Upkeep assigns." />
          </div>
        </section>
      </div>
    </div>
    """
  end

  attr :document, :map, required: true

  def queries_panel(assigns) do
    ~H"""
    <div id="upkeep-queries-panel" class={scroll_panel_class()}>
      <div class="grid gap-3 p-4">
        <section class={card_class()}>
          <.section_header title="Sources & Queries" meta={"#{length(@document.watches)} watches"} />
          <div class="divide-y divide-neutral-200">
            <div :for={watch <- @document.watches} class="px-3 py-3">
              <div class="flex items-start justify-between gap-3">
                <div class="min-w-0">
                  <div class="truncate font-mono text-xs font-semibold text-neutral-950">
                    {watch.source_label}
                  </div>
                  <div class="truncate font-mono text-xs text-neutral-500">
                    {Format.join_or_empty(watch.assign_labels)}
                  </div>
                </div>
                <div class="flex shrink-0 gap-1.5">
                  <.chip label={watch.liveness.label} />
                  <.chip label={watch.dedup.label} />
                </div>
              </div>
              <div class="mt-2 grid grid-cols-[86px_minmax(0,1fr)] gap-x-3 gap-y-1 font-mono text-xs">
                <span class="text-neutral-500">params</span>
                <span class="break-words text-neutral-900">{watch.params_label}</span>
                <span class="text-neutral-500">partition</span>
                <span class="break-words text-neutral-900">{watch.sharing_partition_label}</span>
                <span class="text-neutral-500">queries</span>
                <span class="break-words text-neutral-900">
                  {Format.join_or_empty(watch.tracked_query_labels)}
                </span>
                <span class="text-neutral-500">cache</span>
                <span class="break-words text-neutral-900">{watch.dedup.detail}</span>
              </div>
              <p
                :if={watch.liveness.status != :live_query}
                class="mt-2 mb-0 text-xs leading-5 text-neutral-600"
              >
                {watch.liveness.detail}
              </p>
            </div>
            <.empty_message :if={@document.watches == []} text="No source watches." />
          </div>
        </section>
      </div>
    </div>
    """
  end

  attr :document, :map, required: true

  def invalidation_panel(assigns) do
    ~H"""
    <div id="upkeep-optimization-panel" class={scroll_panel_class()}>
      <div class="grid gap-3 p-4">
        <section class={card_class()}>
          <.section_header title="Invalidation" meta="coverage by source" />
          <div class="divide-y divide-neutral-200">
            <div :for={watch <- @document.watches} class="px-3 py-3">
              <div class="flex items-start justify-between gap-3">
                <div class="min-w-0">
                  <div class="truncate font-mono text-xs font-semibold text-neutral-950">
                    {watch.source_label}
                  </div>
                  <div class="truncate font-mono text-xs text-neutral-500">
                    {Format.join_or_empty(watch.assign_labels)}
                  </div>
                </div>
                <.chip label={watch.liveness.label} />
              </div>
              <div
                :if={watch.coverage}
                class="mt-2 grid grid-cols-[86px_minmax(0,1fr)] gap-x-3 gap-y-1 font-mono text-xs"
              >
                <span class="text-neutral-500">coverage</span>
                <span class="break-words text-neutral-900">{watch.coverage.summary}</span>
                <span :if={watch.coverage.precise != []} class="text-neutral-500">precise</span>
                <span :if={watch.coverage.precise != []} class="break-words text-neutral-900">
                  {Format.join_or_empty(watch.coverage.precise)}
                </span>
                <span :if={watch.coverage.broad != []} class="text-neutral-500">broad</span>
                <span :if={watch.coverage.broad != []} class="break-words text-neutral-900">
                  {Format.join_or_empty(watch.coverage.broad)}
                </span>
                <span :if={watch.coverage.unknown != []} class="text-neutral-500">gaps</span>
                <span :if={watch.coverage.unknown != []} class="break-words text-neutral-900">
                  {Format.join_or_empty(watch.coverage.unknown)}
                </span>
                <span :if={watch.coverage.warnings != []} class="text-neutral-500">warnings</span>
                <span :if={watch.coverage.warnings != []} class="break-words text-neutral-900">
                  {Format.join_or_empty(watch.coverage.warnings)}
                </span>
              </div>
              <div
                :if={!watch.coverage}
                class="mt-2 grid grid-cols-[86px_minmax(0,1fr)] gap-x-3 font-mono text-xs"
              >
                <span class="text-neutral-500">keys</span>
                <span class="break-words text-neutral-900">{watch.interest_keys_label}</span>
              </div>
            </div>
            <.empty_message :if={@document.watches == []} text="No source watches." />
          </div>
        </section>
      </div>
    </div>
    """
  end

  attr :document, :map, required: true
  attr :activity, :map, required: true
  attr :timeline, :list, required: true

  def events_panel(assigns) do
    ~H"""
    <div class={scroll_panel_class()}>
      <div class="grid gap-3 p-4">
        <section class={card_class()}>
          <.section_header title="Last Recompute" meta="latest telemetry" />
          <div class="grid grid-cols-4 gap-px bg-neutral-200 font-mono text-xs max-[900px]:grid-cols-2">
            <.activity_cell label="roots" value={Format.set_label(@activity.roots)} />
            <.activity_cell label="recomputed" value={Format.set_label(@activity.recomputed)} />
            <.activity_cell label="changed" value={Format.set_label(@activity.changed)} />
            <.activity_cell label="skipped" value={Format.set_label(@activity.skipped)} />
          </div>
        </section>

        <section class={card_class()}>
          <.section_header
            title="Pending Refreshes"
            meta={"#{length(@document.pending_refreshes)} queued"}
          />
          <div class="divide-y divide-neutral-200 font-mono text-xs">
            <div
              :for={refresh <- @document.pending_refreshes}
              class="grid grid-cols-[96px_minmax(0,1fr)] gap-3 px-3 py-2"
            >
              <span class="text-neutral-500">root</span>
              <span class="truncate text-neutral-950">{refresh}</span>
            </div>
            <.empty_message :if={@document.pending_refreshes == []} text="Nothing queued." />
          </div>
        </section>

        <section id="upkeep-timeline-panel" class={card_class()}>
          <.section_header title="Events" meta={"#{length(@timeline)} captured"} />
          <div class="divide-y divide-neutral-200">
            <div
              :for={event <- @timeline}
              class="grid grid-cols-[62px_58px_minmax(160px,0.8fr)_minmax(0,1.2fr)] items-baseline gap-3 px-3 py-2 font-mono text-xs hover:bg-neutral-50"
            >
              <span class="text-neutral-400">{event.time_label}</span>
              <span class="rounded border border-neutral-200 bg-neutral-50 px-1.5 py-0.5 text-center text-neutral-700">{event.tag}</span>
              <span class="min-w-0 break-words text-neutral-950">{event.name_label}</span>
              <span class="min-w-0 truncate text-neutral-600">{event.metadata_label}</span>
            </div>
            <.empty_message :if={@timeline == []} text="No telemetry events." />
          </div>
        </section>
      </div>
    </div>
    """
  end

  attr :layout, :map, required: true
  attr :code, :string, required: true
  attr :snapshot, :string, required: true

  def source_panel(assigns) do
    ~H"""
    <div id="upkeep-source-panel" class={scroll_panel_class()}>
      <div class="grid grid-cols-[minmax(0,1fr)_minmax(320px,0.8fr)] gap-3 p-4 max-[1100px]:grid-cols-1">
        <section class={card_class()}>
          <.section_header title="Captured Source" meta="DSL callsites" />
          <div class="divide-y divide-neutral-200">
            <div
              :for={node <- @layout.nodes}
              :if={node.source_location}
              class="px-3 py-3"
            >
              <div class="mb-2 flex items-center justify-between gap-3">
                <span class="truncate font-mono text-xs font-semibold text-neutral-950">
                  {node.detail}
                </span>
                <span class="truncate font-mono text-xs text-neutral-500">
                  {node.source_location.location_label}
                </span>
              </div>
              <pre class="max-h-56 overflow-auto rounded-md bg-neutral-950 px-3 py-2 font-mono text-xs leading-5 text-neutral-100"><code>{node.source_location.code}</code></pre>
            </div>
            <.empty_message
              :if={not Enum.any?(@layout.nodes, &Map.get(&1, :source_location))}
              text="No callsites captured."
            />
          </div>
        </section>

        <div class="grid min-h-0 gap-3">
          <section class={card_class()}>
            <.section_header title="Declaration" meta="symbolic" />
            <pre class="max-h-80 overflow-auto p-3 font-mono text-xs leading-5 text-neutral-900"><code>{@code}</code></pre>
          </section>

          <section class={card_class()}>
            <.section_header title="Snapshot" meta="graph_snapshot/1" />
            <pre class="max-h-80 overflow-auto p-3 font-mono text-xs leading-5 text-neutral-900"><code>{@snapshot}</code></pre>
          </section>
        </div>
      </div>
    </div>
    """
  end

  attr :activity, :map, required: true
  attr :layout, :map, required: true

  def node_drawer(assigns) do
    ~H"""
    <aside
      id="upkeep-node-inspector-panel"
      class="flex min-h-0 flex-col overflow-hidden bg-white max-[1100px]:max-h-[620px]"
    >
      <.panel_header title="Node" meta={"#{length(@layout.nodes)} total"} />
      <div class="border-b border-neutral-200 p-3">
        <div class="grid grid-cols-2 gap-2 font-mono text-xs">
          <.mini_stat label="roots" value={MapSet.size(@activity.roots)} />
          <.mini_stat label="changed" value={MapSet.size(@activity.changed)} />
          <.mini_stat label="recompute" value={MapSet.size(@activity.recomputed)} />
          <.mini_stat label="skipped" value={MapSet.size(@activity.skipped)} />
        </div>
      </div>
      <div class="min-h-0 flex-1 overflow-auto p-3">
        <div
          :for={node <- @layout.nodes}
          id={node.inspect_dom_id}
          class="mb-2 scroll-mt-3 rounded-md border border-neutral-200 bg-white p-3 last:mb-0 target:border-blue-600"
        >
          <div class="flex items-start justify-between gap-3">
            <a
              href={"##{node.dom_id}"}
              class="min-w-0 break-words font-mono text-xs font-semibold text-neutral-950 no-underline"
            >
              {node.detail}
            </a>
            <.chip label={node.state_label} active?={Format.state_active?(node.state)} />
          </div>
          <p class="mt-2 mb-0 text-xs leading-5 text-neutral-800">
            {node.explanation.headline}
          </p>
          <div class="mt-2 grid grid-cols-[58px_minmax(0,1fr)] gap-x-2 gap-y-1 font-mono text-xs">
            <span class="text-neutral-500">in</span>
            <span class="break-words text-neutral-800">
              {Format.join_or_empty(node.explanation.inputs)}
            </span>
            <span class="text-neutral-500">out</span>
            <span class="break-words text-neutral-800">
              {Format.join_or_empty(node.explanation.outputs)}
            </span>
          </div>
          <details class="mt-2 rounded-md border border-neutral-200 bg-neutral-50 px-2.5 py-2">
            <summary class="cursor-pointer text-xs font-semibold uppercase text-neutral-500">
              Details
            </summary>
            <p class="mt-2 mb-0 text-xs leading-5 text-neutral-700">{node.explanation.body}</p>
            <div class="mt-2 rounded-r border-l-2 border-neutral-300 bg-white px-2.5 py-2 text-xs leading-5 text-neutral-800">
              <div class="flex items-center justify-between gap-2">
                <span>{node.optimization.detail}</span>
                <.chip label={node.optimization.label} />
              </div>
              <ul
                :if={node.optimization.bullets != []}
                class="mt-1.5 mb-0 list-none space-y-1 p-0 font-mono text-xs text-neutral-600"
              >
                <li :for={bullet <- node.optimization.bullets}>{bullet}</li>
              </ul>
            </div>
            <div class="mt-2 grid grid-cols-[70px_minmax(0,1fr)] gap-x-2 gap-y-1 font-mono text-xs">
              <span class="text-neutral-500">kind</span>
              <span class="break-words text-neutral-800">{node.kind_label}</span>
              <span class="text-neutral-500">scope</span>
              <span><.chip label={Format.scope_label(node.scope)} /></span>
              <span class="text-neutral-500">deps</span>
              <span class="break-words text-neutral-800">{Format.join_or_empty(node.deps)}</span>
            </div>
          </details>
        </div>
      </div>
    </aside>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, required: true

  defp mini_stat(assigns) do
    ~H"""
    <div class="rounded-md border border-neutral-200 bg-neutral-50 px-2.5 py-2">
      <div class="text-sm font-semibold leading-none text-neutral-950">{@value}</div>
      <div class="mt-1 text-xs uppercase text-neutral-500">{@label}</div>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true

  defp activity_cell(assigns) do
    ~H"""
    <div class="min-w-0 bg-white p-3">
      <div class="text-xs uppercase text-neutral-500">{@label}</div>
      <div class="mt-1 truncate text-neutral-950">{@value}</div>
    </div>
    """
  end

  attr :title, :string, required: true
  attr :meta, :string, required: true

  defp section_header(assigns) do
    ~H"""
    <div class="flex items-center justify-between gap-3 border-b border-neutral-200 px-3 py-2">
      <h2 class="m-0 text-xs font-semibold uppercase text-neutral-950">{@title}</h2>
      <span class="truncate font-mono text-xs text-neutral-400">{@meta}</span>
    </div>
    """
  end

  attr :title, :string, required: true
  attr :meta, :string, required: true

  defp panel_header(assigns) do
    ~H"""
    <div class="flex items-center justify-between border-b border-neutral-200 bg-white px-3.5 py-2.5">
      <h2 class="m-0 text-xs font-semibold uppercase text-neutral-950">{@title}</h2>
      <span class="font-mono text-xs text-neutral-400">{@meta}</span>
    </div>
    """
  end

  attr :text, :string, required: true

  defp empty_message(assigns) do
    ~H"""
    <div class="p-3 text-center text-xs text-neutral-400">{@text}</div>
    """
  end

  attr :label, :string, required: true
  attr :active?, :boolean, default: false

  defp chip(assigns) do
    ~H"""
    <span class={[
      "inline-flex shrink-0 items-center rounded-full border px-2 py-0.5 font-mono text-xs leading-tight",
      if(@active?,
        do: "border-blue-600 bg-blue-50 text-blue-700",
        else: "border-neutral-200 bg-white text-neutral-600"
      )
    ]}>{@label}</span>
    """
  end

  defp scroll_panel_class do
    "h-full min-h-0 overflow-auto bg-neutral-50"
  end

  defp card_class do
    "overflow-hidden rounded-md border border-neutral-200 bg-white"
  end

  defp assign_row_class(assign) do
    [
      "grid grid-cols-[minmax(100px,0.7fr)_minmax(0,1fr)_auto] items-center gap-3 px-3 py-2",
      Map.get(assign, :changed?) && "changed bg-emerald-50"
    ]
  end
end
