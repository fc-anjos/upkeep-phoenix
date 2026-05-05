defmodule Upkeep.Live.Inspector.Panels do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Live.Inspector.{Activity, Format}

  attr :document, :map, required: true

  def playground_panel(assigns) do
    ~H"""
    <aside class={panel_class()} id="upkeep-playground-panel">
      <.panel_header title="Playground" meta="socket.assigns" />
      <div class="space-y-5 p-3.5">
        <section>
          <h3 class={section_title_class()}>Rendered LiveView State</h3>
          <div class="overflow-hidden rounded-md border border-neutral-200 bg-neutral-100 font-mono text-[11px]">
            <div class="flex justify-between border-b border-neutral-200 bg-neutral-200 px-2.5 py-1.5 text-neutral-600">
              <span>symbolic assigns</span>
              <span>{length(@document.assigns)} keys</span>
            </div>
            <div>
              <div
                :for={assign <- @document.assigns}
                id={assign.dom_id}
                class={assign_row_class(assign)}
              >
                <span class="min-w-0 truncate text-neutral-600">{assign.label}</span>
                <span class="min-w-0 truncate text-neutral-950">
                  {Format.shape_label(assign.shape)}
                </span>
              </div>
              <.empty_message :if={@document.assigns == []} text="No Upkeep assigns registered." />
            </div>
          </div>
        </section>

        <section>
          <h3 class={section_title_class()}>Source Watches</h3>
          <div class="space-y-2.5">
            <div
              :for={watch <- @document.watches}
              class="rounded-md border border-neutral-200 bg-white p-3 shadow-sm"
            >
              <div class="flex items-start justify-between gap-3">
                <span class="min-w-0 truncate font-mono text-xs font-semibold text-neutral-950">
                  {watch.source_label}
                </span>
                <.scope_badge scope={Activity.watch_scope(watch)} />
              </div>
              <div class="mt-2 grid grid-cols-[92px_minmax(0,1fr)] gap-x-2.5 gap-y-1 font-mono text-[11px]">
                <span class="text-neutral-500">assigns</span>
                <span class="min-w-0 break-words text-neutral-900">
                  {Format.join_or_empty(watch.assign_labels)}
                </span>
                <span class="text-neutral-500">params</span>
                <span class="min-w-0 break-words text-neutral-900">{watch.params_label}</span>
                <span class="text-neutral-500">partition</span>
                <span class="min-w-0 break-words text-neutral-900">
                  {watch.sharing_partition_label}
                </span>
              </div>
            </div>
          </div>
          <.empty_message :if={@document.watches == []} text="No source watches are active." />
        </section>

        <section>
          <h3 class={section_title_class()}>Pending Refreshes</h3>
          <div class="overflow-hidden rounded-md border border-neutral-200 bg-neutral-100 font-mono text-[11px]">
            <div class="flex justify-between border-b border-neutral-200 bg-neutral-200 px-2.5 py-1.5 text-neutral-600">
              <span>queued roots</span>
              <span>{length(@document.pending_refreshes)}</span>
            </div>
            <div
              :for={refresh <- @document.pending_refreshes}
              class="grid grid-cols-[88px_minmax(0,1fr)] gap-2 border-b border-dashed border-neutral-200 px-2.5 py-1.5 last:border-0"
            >
              <span class="text-neutral-600">root</span>
              <span class="min-w-0 truncate text-neutral-950">{refresh}</span>
            </div>
            <.empty_message :if={@document.pending_refreshes == []} text="Nothing queued." />
          </div>
        </section>
      </div>
    </aside>
    """
  end

  attr :activity, :map, required: true
  attr :layout, :map, required: true
  attr :snapshot, :string, required: true

  def node_inspector_panel(assigns) do
    ~H"""
    <aside class={panel_class()} id="upkeep-node-inspector-panel">
      <.panel_header title="Inspector" meta="graph_snapshot/1" />
      <div class="space-y-5 p-3.5">
        <section>
          <h3 class={section_title_class()}>Last Event</h3>
          <div class="rounded-md border border-neutral-200 bg-white p-3 shadow-sm">
            <div class="grid grid-cols-[96px_minmax(0,1fr)] gap-x-2.5 gap-y-1 font-mono text-[11px]">
              <span class="text-neutral-500">roots</span>
              <span class="min-w-0 break-words text-neutral-900">
                {Format.set_label(@activity.roots)}
              </span>
              <span class="text-neutral-500">recomputed</span>
              <span class="min-w-0 break-words text-neutral-900">
                {Format.set_label(@activity.recomputed)}
              </span>
              <span class="text-neutral-500">changed</span>
              <span class="min-w-0 break-words text-neutral-900">
                {Format.set_label(@activity.changed)}
              </span>
              <span class="text-neutral-500">skipped</span>
              <span class="min-w-0 break-words text-neutral-900">
                {Format.set_label(@activity.skipped)}
              </span>
            </div>
          </div>
        </section>

        <section>
          <h3 class={section_title_class()}>Node Details</h3>
          <div
            :for={node <- @layout.nodes}
            id={node.inspect_dom_id}
            class={[
              "mt-2 scroll-mt-3 rounded-md border border-neutral-200 bg-white p-3 shadow-sm transition-colors first:mt-0 target:border-[oklch(0.55_0.15_250)] target:bg-[oklch(0.95_0.04_250)]"
            ]}
          >
            <div class="flex items-start justify-between gap-3">
              <a
                href={"##{node.dom_id}"}
                class="min-w-0 break-words font-mono text-xs font-semibold text-neutral-950 no-underline"
              >
                {node.detail}
              </a>
              <.state_badge state={node.state} label={node.state_label} />
            </div>
            <div class="mt-2 grid grid-cols-[96px_minmax(0,1fr)] gap-x-2.5 gap-y-1 font-mono text-[11px]">
              <span class="text-neutral-500">id</span>
              <span class="min-w-0 break-words text-neutral-900">{node.id_label}</span>
              <span class="text-neutral-500">kind</span>
              <span class="min-w-0 break-words text-neutral-900">{node.kind_label}</span>
              <span class="text-neutral-500">assigns</span>
              <span class="min-w-0 break-words text-neutral-900">
                {Format.assign_label(node.assign_names)}
              </span>
              <span class="text-neutral-500">deps</span>
              <span class="min-w-0 break-words text-neutral-900">
                {Format.join_or_empty(node.deps)}
              </span>
              <span class="text-neutral-500">dependents</span>
              <span class="min-w-0 break-words text-neutral-900">
                {Format.join_or_empty(node.dependents)}
              </span>
              <span class="text-neutral-500">scope</span>
              <span class="min-w-0 text-neutral-900"><.scope_badge scope={node.scope} /></span>
            </div>
            <div class={reason_class(node.state)}>{node.reason}</div>
            <div
              :if={node.source_location}
              class="mt-2.5 overflow-hidden rounded-md border border-neutral-200 bg-neutral-950 shadow-sm"
            >
              <div class="flex items-center justify-between gap-3 border-b border-white/10 px-2.5 py-1.5">
                <span class="text-[10px] font-bold uppercase tracking-wider text-neutral-300">
                  Captured Source
                </span>
                <span class="min-w-0 truncate font-mono text-[10px] text-neutral-400">
                  {node.source_location.location_label}
                </span>
              </div>
              <pre class="max-h-40 overflow-auto whitespace-pre-wrap px-2.5 py-2 font-mono text-[10px] leading-4 text-neutral-100"><code>{node.source_location.code}</code></pre>
            </div>
          </div>
        </section>

        <section>
          <h3 class={section_title_class()}>Snapshot</h3>
          <div class="rounded-md border border-neutral-200 bg-white p-3 shadow-sm">
            <pre class="overflow-auto whitespace-pre-wrap font-mono text-[11px] leading-5 text-neutral-900"><code>{@snapshot}</code></pre>
          </div>
        </section>
      </div>
    </aside>
    """
  end

  attr :timeline, :list, required: true

  def timeline_panel(assigns) do
    ~H"""
    <div class={panel_class()} id="upkeep-timeline-panel">
      <.panel_header title="Event Timeline" meta={"#{length(@timeline)} events"} />
      <div class="min-h-0 overflow-auto">
        <div
          :for={event <- @timeline}
          class="grid grid-cols-[62px_70px_minmax(160px,0.8fr)_minmax(0,1.2fr)] items-baseline gap-3 border-b border-neutral-200 px-3.5 py-2 font-mono text-[11px] transition-colors hover:bg-neutral-100"
        >
          <span class="text-neutral-400">{event.time_label}</span>
          <span class={timeline_tag_class(event.tag)}>{event.tag}</span>
          <span class="min-w-0 break-words text-neutral-950">{event.name_label}</span>
          <span class="min-w-0 truncate text-neutral-600">{event.metadata_label}</span>
        </div>
        <.empty_message :if={@timeline == []} text="No telemetry events captured." />
      </div>
    </div>
    """
  end

  attr :code, :string, required: true

  def code_panel(assigns) do
    ~H"""
    <div class={panel_class(["code-panel"])} id="upkeep-code-panel">
      <.panel_header title="Equivalent Upkeep declaration" meta="symbolic" />
      <pre class="min-h-0 overflow-auto whitespace-pre p-3.5 font-mono text-xs leading-5 text-neutral-950"><code>{@code}</code></pre>
    </div>
    """
  end

  attr :title, :string, required: true
  attr :meta, :string, required: true

  defp panel_header(assigns) do
    ~H"""
    <div class="sticky top-0 z-10 flex items-center justify-between border-b border-neutral-200 bg-white px-3.5 py-2.5">
      <h2 class="m-0 text-xs font-bold uppercase tracking-wide text-neutral-950">{@title}</h2>
      <span class="font-mono text-[11px] text-neutral-400">{@meta}</span>
    </div>
    """
  end

  attr :text, :string, required: true

  defp empty_message(assigns) do
    ~H"""
    <div class="rounded-md border border-dashed border-neutral-200 p-3.5 text-center text-xs text-neutral-400">
      {@text}
    </div>
    """
  end

  attr :scope, :atom, required: true

  defp scope_badge(assigns) do
    assigns = assign(assigns, :label, Format.scope_label(assigns.scope))

    ~H"""
    <span class={chip_class(Format.scope_class(@scope))}>{@label}</span>
    """
  end

  attr :state, :atom, required: true
  attr :label, :string, required: true

  defp state_badge(assigns) do
    ~H"""
    <span class={chip_class(Format.state_class(@state))}>{@label}</span>
    """
  end

  defp panel_class(extra \\ []) do
    [
      "flex min-h-0 flex-col overflow-auto border-r border-neutral-200 bg-white last:border-r-0 max-[1100px]:border-b max-[1100px]:border-r-0",
      extra
    ]
  end

  defp section_title_class do
    "mb-2 text-[11px] font-bold uppercase tracking-wider text-neutral-600"
  end

  defp assign_row_class(assign) do
    [
      "lv-row grid grid-cols-[minmax(88px,0.9fr)_minmax(0,1fr)] items-baseline gap-2 border-b border-dashed border-neutral-200 px-2.5 py-1.5 last:border-0",
      Map.get(assign, :changed?) && "changed bg-emerald-50"
    ]
  end

  defp reason_class(state) do
    [
      "mt-2.5 rounded-r border-l-2 bg-neutral-100 px-2.5 py-2 text-xs leading-5 text-neutral-900",
      case Format.reason_class(state) do
        "blue" -> "border-[oklch(0.55_0.15_250)]"
        "amber" -> "border-[oklch(0.68_0.15_70)]"
        "green" -> "border-[oklch(0.6_0.15_145)]"
        _ -> "border-neutral-300"
      end
    ]
  end

  defp chip_class(color) do
    [
      "inline-flex shrink-0 items-center rounded-full border px-2 py-1 font-mono text-[11px] leading-none",
      case color do
        "blue" ->
          "border-transparent bg-[oklch(0.95_0.04_250)] text-[oklch(0.55_0.15_250)]"

        "amber" ->
          "border-transparent bg-[oklch(0.96_0.05_70)] text-[oklch(0.45_0.15_70)]"

        "green" ->
          "border-transparent bg-[oklch(0.95_0.05_145)] text-[oklch(0.42_0.15_145)]"

        "teal" ->
          "border-transparent bg-[oklch(0.95_0.03_195)] text-[oklch(0.42_0.13_195)]"

        "orange" ->
          "border-transparent bg-[oklch(0.95_0.04_35)] text-[oklch(0.45_0.13_35)]"

        _ ->
          "border-neutral-200 bg-white text-neutral-500"
      end
    ]
  end

  defp timeline_tag_class(tag) do
    [
      "rounded border px-1.5 py-0.5 text-center text-[10px]",
      case tag do
        "diff" ->
          "border-transparent bg-[oklch(0.95_0.05_145)] text-[oklch(0.42_0.15_145)]"

        "err" ->
          "border-transparent bg-[oklch(0.95_0.04_25)] text-[oklch(0.55_0.18_25)]"

        _ ->
          "border-transparent bg-[oklch(0.95_0.03_195)] text-[oklch(0.42_0.13_195)]"
      end
    ]
  end
end
