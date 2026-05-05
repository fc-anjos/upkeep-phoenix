defmodule Upkeep.Live.Inspector.Components do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Live.Inspector.{GraphComponent, Panels}

  def page(assigns) do
    ~H"""
    <div
      id="upkeep-inspector"
      class={[
        "grid h-screen min-h-[720px] grid-rows-[56px_minmax(0,1fr)] overflow-hidden bg-neutral-50 text-[13px] text-neutral-950 antialiased",
        "[font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe_UI',system-ui,sans-serif]",
        "max-[1100px]:h-auto max-[1100px]:min-h-screen max-[1100px]:overflow-visible"
      ]}
    >
      <.header document={@upkeep_document} />

      <main class="grid min-h-0 grid-cols-[minmax(0,1fr)_380px] overflow-hidden max-[1100px]:grid-cols-1">
        <.tabbed_workspace
          document={@upkeep_document}
          activity={@upkeep_activity}
          layout={@upkeep_layout}
          timeline={@upkeep_timeline}
          code={@upkeep_code}
          snapshot={@upkeep_snapshot}
        />
        <Panels.node_drawer
          activity={@upkeep_activity}
          layout={@upkeep_layout}
        />
      </main>
    </div>
    """
  end

  attr :document, :map, required: true
  attr :activity, :map, required: true
  attr :layout, :map, required: true
  attr :timeline, :list, required: true
  attr :code, :string, required: true
  attr :snapshot, :string, required: true

  defp tabbed_workspace(assigns) do
    ~H"""
    <section
      id="upkeep-tabbed-workspace"
      class="grid min-h-0 grid-cols-7 grid-rows-[42px_minmax(0,1fr)] overflow-hidden border-r border-neutral-200 bg-neutral-100 max-[1100px]:border-b max-[1100px]:border-r-0"
    >
      <style phx-no-curly-interpolation>
        #upkeep-tab-overview:checked,
        #upkeep-tab-flow:checked,
        #upkeep-tab-data:checked,
        #upkeep-tab-queries:checked,
        #upkeep-tab-invalidation:checked,
        #upkeep-tab-events:checked,
        #upkeep-tab-source:checked {
          background: #fff;
          border-bottom-color: #fff;
          box-shadow: inset 0 -2px 0 #0a0a0a;
        }

        #upkeep-tab-overview:checked ~ #upkeep-tab-overview-control,
        #upkeep-tab-flow:checked ~ #upkeep-tab-flow-control,
        #upkeep-tab-data:checked ~ #upkeep-tab-data-control,
        #upkeep-tab-queries:checked ~ #upkeep-tab-queries-control,
        #upkeep-tab-invalidation:checked ~ #upkeep-tab-invalidation-control,
        #upkeep-tab-events:checked ~ #upkeep-tab-events-control,
        #upkeep-tab-source:checked ~ #upkeep-tab-source-control {
          color: #0a0a0a;
        }

        #upkeep-tab-overview:checked ~ #upkeep-overview-tab,
        #upkeep-tab-flow:checked ~ #upkeep-flow-tab,
        #upkeep-tab-data:checked ~ #upkeep-data-tab,
        #upkeep-tab-queries:checked ~ #upkeep-queries-tab,
        #upkeep-tab-invalidation:checked ~ #upkeep-invalidation-tab,
        #upkeep-tab-events:checked ~ #upkeep-events-tab,
        #upkeep-tab-source:checked ~ #upkeep-source-tab {
          display: block;
        }
      </style>

      <input
        id="upkeep-tab-overview"
        class={tab_radio_class(["col-start-1"])}
        type="radio"
        name="upkeep-inspector-tab"
        aria-label="Overview"
        checked
      />
      <input
        id="upkeep-tab-flow"
        class={tab_radio_class(["col-start-2"])}
        type="radio"
        name="upkeep-inspector-tab"
        aria-label="Flow"
      />
      <input
        id="upkeep-tab-data"
        class={tab_radio_class(["col-start-3"])}
        type="radio"
        name="upkeep-inspector-tab"
        aria-label="Data"
      />
      <input
        id="upkeep-tab-queries"
        class={tab_radio_class(["col-start-4"])}
        type="radio"
        name="upkeep-inspector-tab"
        aria-label="Queries"
      />
      <input
        id="upkeep-tab-invalidation"
        class={tab_radio_class(["col-start-5"])}
        type="radio"
        name="upkeep-inspector-tab"
        aria-label="Invalidation"
      />
      <input
        id="upkeep-tab-events"
        class={tab_radio_class(["col-start-6"])}
        type="radio"
        name="upkeep-inspector-tab"
        aria-label="Events"
      />
      <input
        id="upkeep-tab-source"
        class={tab_radio_class(["col-start-7"])}
        type="radio"
        name="upkeep-inspector-tab"
        aria-label="Source"
      />

      <label
        id="upkeep-tab-overview-control"
        for="upkeep-tab-overview"
        class={tab_label_class(["col-start-1"])}
      >
        Overview
      </label>
      <label
        id="upkeep-tab-flow-control"
        for="upkeep-tab-flow"
        class={tab_label_class(["col-start-2"])}
      >
        Flow
      </label>
      <label
        id="upkeep-tab-data-control"
        for="upkeep-tab-data"
        class={tab_label_class(["col-start-3"])}
      >
        Data
      </label>
      <label
        id="upkeep-tab-queries-control"
        for="upkeep-tab-queries"
        class={tab_label_class(["col-start-4"])}
      >
        Queries
      </label>
      <label
        id="upkeep-tab-invalidation-control"
        for="upkeep-tab-invalidation"
        class={tab_label_class(["col-start-5"])}
      >
        Invalidation
      </label>
      <label
        id="upkeep-tab-events-control"
        for="upkeep-tab-events"
        class={tab_label_class(["col-start-6"])}
      >
        Events
      </label>
      <label
        id="upkeep-tab-source-control"
        for="upkeep-tab-source"
        class={tab_label_class(["col-start-7"])}
      >
        Source
      </label>

      <div id="upkeep-overview-tab" class={tab_panel_class()}>
        <Panels.overview_panel document={@document} activity={@activity} />
      </div>
      <div id="upkeep-flow-tab" class={tab_panel_class()}>
        <GraphComponent.graph_panel document={@document} layout={@layout} />
      </div>
      <div id="upkeep-data-tab" class={tab_panel_class()}>
        <Panels.data_panel document={@document} />
      </div>
      <div id="upkeep-queries-tab" class={tab_panel_class()}>
        <Panels.queries_panel document={@document} />
      </div>
      <div id="upkeep-invalidation-tab" class={tab_panel_class()}>
        <Panels.invalidation_panel document={@document} />
      </div>
      <div id="upkeep-events-tab" class={tab_panel_class()}>
        <Panels.events_panel
          document={@document}
          activity={@activity}
          timeline={@timeline}
        />
      </div>
      <div id="upkeep-source-tab" class={tab_panel_class()}>
        <Panels.source_panel layout={@layout} code={@code} snapshot={@snapshot} />
      </div>
    </section>
    """
  end

  attr :document, :map, required: true

  defp header(assigns) do
    ~H"""
    <header class="flex items-center gap-3.5 border-b border-neutral-200 bg-white px-5">
      <div
        class="flex h-[22px] w-[22px] items-center justify-center rounded-[5px] bg-[radial-gradient(circle_at_30%_30%,oklch(0.55_0.15_250),oklch(0.45_0.16_260))]"
        aria-hidden="true"
      >
        <svg width="14" height="14" viewBox="0 0 14 14">
          <circle cx="3" cy="3" r="1.5" fill="white" />
          <circle cx="11" cy="3" r="1.5" fill="white" />
          <circle cx="7" cy="11" r="1.5" fill="white" />
          <line x1="3" y1="3" x2="7" y2="11" stroke="white" stroke-width="0.8" />
          <line x1="11" y1="3" x2="7" y2="11" stroke="white" stroke-width="0.8" />
        </svg>
      </div>

      <div class="min-w-0">
        <h1 class="m-0 text-[15px] font-semibold leading-tight text-neutral-950">
          Upkeep Inspector
        </h1>
        <div class="mt-0.5 truncate text-xs text-neutral-600">
          {@document.live_view.view_label || "Phoenix LiveView runtime"} - {@document.summary.data_sentence}
        </div>
      </div>

      <div class="ml-auto flex items-center gap-1.5 max-[760px]:hidden">
        <span class="inline-flex items-center gap-1.5 rounded-full border border-neutral-200 bg-white px-2 py-1 font-mono text-[11px] leading-none text-neutral-600">
          <span class="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-500"></span> real LiveView
        </span>
        <span class="rounded-full border border-neutral-200 bg-white px-2 py-1 font-mono text-[11px] leading-none text-neutral-600">
          runtime snapshot
        </span>
        <span class="rounded-full border border-neutral-200 bg-white px-2 py-1 font-mono text-[11px] leading-none text-neutral-600">
          telemetry-backed
        </span>
      </div>
    </header>
    """
  end

  defp tab_label_class(extra) do
    [
      "pointer-events-none z-10 row-start-1 flex items-center justify-center px-2 text-[11px] font-semibold uppercase tracking-wide text-neutral-500 transition-colors",
      "max-[760px]:text-[10px]",
      extra
    ]
  end

  defp tab_radio_class(extra) do
    [
      "z-0 row-start-1 h-full w-full cursor-pointer appearance-none border-b border-r border-neutral-200 bg-neutral-100 transition-colors hover:bg-white",
      "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-[-2px] focus-visible:outline-neutral-950",
      extra
    ]
  end

  defp tab_panel_class,
    do: "col-span-7 row-start-2 hidden min-h-0 overflow-hidden bg-neutral-50"
end
