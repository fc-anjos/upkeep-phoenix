defmodule Upkeep.Live.Inspector.Components do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Live.Inspector.{GraphComponent, Panels}

  def page(assigns) do
    ~H"""
    <div
      id="upkeep-inspector"
      class={[
        "grid h-screen min-h-[680px] grid-rows-[44px_minmax(0,1fr)] overflow-hidden bg-white text-sm text-neutral-900 antialiased",
        "[font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe_UI',system-ui,sans-serif]",
        "max-[1100px]:h-auto max-[1100px]:min-h-screen max-[1100px]:overflow-visible"
      ]}
    >
      <.toolbar document={@upkeep_document} />

      <main class="grid min-h-0 grid-cols-[minmax(0,1fr)_360px] overflow-hidden max-[1100px]:grid-cols-1">
        <.tabbed_workspace
          document={@upkeep_document}
          activity={@upkeep_activity}
          layout={@upkeep_layout}
          timeline={@upkeep_timeline}
          code={@upkeep_code}
          snapshot={@upkeep_snapshot}
        />
        <Panels.node_drawer activity={@upkeep_activity} layout={@upkeep_layout} />
      </main>
    </div>
    """
  end

  attr :document, :map, required: true

  defp toolbar(assigns) do
    connected? = get_in(assigns, [:document, Access.key(:live_view), Access.key(:connected?)]) || false
    counts = assigns.document.summary.counts

    assigns =
      assigns
      |> assign(:connected?, connected?)
      |> assign(:counts, counts)

    ~H"""
    <header class="flex h-11 items-center gap-4 border-b border-neutral-200 bg-white px-5">
      <h1 class="m-0 flex items-baseline gap-2 text-sm font-semibold leading-none text-neutral-950">
        <span>Upkeep Inspector</span>
      </h1>
      <span class="truncate font-mono text-xs leading-none text-neutral-500">
        {@document.live_view.view_label || "Phoenix.LiveView"}
      </span>

      <span class="ml-auto flex items-center gap-5 text-xs text-neutral-500 max-[760px]:hidden">
        <span class="font-mono">
          <span class="text-neutral-950">{@counts.assigns}</span> assigns
        </span>
        <span class="font-mono">
          <span class="text-neutral-950">{@counts.sources}</span> sources
        </span>
        <span class="font-mono">
          <span class="text-neutral-950">{@counts.nodes}</span> nodes
        </span>
        <span class="flex items-center gap-1.5">
          <span class={[
            "h-1.5 w-1.5 rounded-full",
            if(@connected?, do: "bg-emerald-500", else: "bg-neutral-300")
          ]}></span>
          <span class={if(@connected?, do: "text-emerald-700", else: "text-neutral-500")}>
            {if(@connected?, do: "connected", else: "offline")}
          </span>
        </span>
      </span>
    </header>
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
      class="grid min-h-0 grid-cols-7 grid-rows-[36px_minmax(0,1fr)] overflow-hidden border-r border-neutral-200 bg-white max-[1100px]:border-b max-[1100px]:border-r-0"
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
          box-shadow: inset 0 -2px 0 #2563eb;
        }

        #upkeep-tab-overview:checked ~ #upkeep-tab-overview-control,
        #upkeep-tab-flow:checked ~ #upkeep-tab-flow-control,
        #upkeep-tab-data:checked ~ #upkeep-tab-data-control,
        #upkeep-tab-queries:checked ~ #upkeep-tab-queries-control,
        #upkeep-tab-invalidation:checked ~ #upkeep-tab-invalidation-control,
        #upkeep-tab-events:checked ~ #upkeep-tab-events-control,
        #upkeep-tab-source:checked ~ #upkeep-tab-source-control {
          color: #0a0a0a;
          font-weight: 600;
        }

        #upkeep-tab-overview:checked ~ #upkeep-overview-tab,
        #upkeep-tab-flow:checked ~ #upkeep-flow-tab,
        #upkeep-tab-data:checked ~ #upkeep-data-tab,
        #upkeep-tab-queries:checked ~ #upkeep-queries-tab,
        #upkeep-tab-invalidation:checked ~ #upkeep-invalidation-tab,
        #upkeep-tab-events:checked ~ #upkeep-events-tab,
        #upkeep-tab-source:checked ~ #upkeep-source-tab {
          display: flex;
        }
      </style>

      <input id="upkeep-tab-overview" class={tab_radio_class(["col-start-1"])} type="radio" name="upkeep-inspector-tab" aria-label="Overview" checked />
      <input id="upkeep-tab-flow" class={tab_radio_class(["col-start-2"])} type="radio" name="upkeep-inspector-tab" aria-label="Flow" />
      <input id="upkeep-tab-data" class={tab_radio_class(["col-start-3"])} type="radio" name="upkeep-inspector-tab" aria-label="Data" />
      <input id="upkeep-tab-queries" class={tab_radio_class(["col-start-4"])} type="radio" name="upkeep-inspector-tab" aria-label="Queries" />
      <input id="upkeep-tab-invalidation" class={tab_radio_class(["col-start-5"])} type="radio" name="upkeep-inspector-tab" aria-label="Invalidation" />
      <input id="upkeep-tab-events" class={tab_radio_class(["col-start-6"])} type="radio" name="upkeep-inspector-tab" aria-label="Events" />
      <input id="upkeep-tab-source" class={tab_radio_class(["col-start-7"])} type="radio" name="upkeep-inspector-tab" aria-label="Source" />

      <label id="upkeep-tab-overview-control" for="upkeep-tab-overview" class={tab_label_class(["col-start-1"])}>Overview</label>
      <label id="upkeep-tab-flow-control" for="upkeep-tab-flow" class={tab_label_class(["col-start-2"])}>Flow</label>
      <label id="upkeep-tab-data-control" for="upkeep-tab-data" class={tab_label_class(["col-start-3"])}>Data</label>
      <label id="upkeep-tab-queries-control" for="upkeep-tab-queries" class={tab_label_class(["col-start-4"])}>Queries</label>
      <label id="upkeep-tab-invalidation-control" for="upkeep-tab-invalidation" class={tab_label_class(["col-start-5"])}>Invalidation</label>
      <label id="upkeep-tab-events-control" for="upkeep-tab-events" class={tab_label_class(["col-start-6"])}>Events</label>
      <label id="upkeep-tab-source-control" for="upkeep-tab-source" class={tab_label_class(["col-start-7"])}>Source</label>

      <div id="upkeep-overview-tab" class={tab_panel_class()}>
        <Panels.overview_panel document={@document} activity={@activity} timeline={@timeline} />
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
        <Panels.events_panel document={@document} activity={@activity} timeline={@timeline} />
      </div>
      <div id="upkeep-source-tab" class={tab_panel_class()}>
        <Panels.source_panel layout={@layout} code={@code} snapshot={@snapshot} />
      </div>
    </section>
    """
  end

  defp tab_label_class(extra) do
    [
      "pointer-events-none z-10 row-start-1 flex items-center justify-center text-xs text-neutral-500",
      extra
    ]
  end

  defp tab_radio_class(extra) do
    [
      "z-0 row-start-1 h-full w-full cursor-pointer appearance-none border-b border-r border-neutral-200 bg-neutral-50 hover:bg-white",
      "last:border-r-0",
      extra
    ]
  end

  defp tab_panel_class,
    do: "col-span-7 row-start-2 hidden min-h-0 flex-col overflow-hidden bg-white"
end
