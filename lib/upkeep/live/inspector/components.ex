defmodule Upkeep.Live.Inspector.Components do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Live.Inspector.{GraphComponent, Panels}

  def page(assigns) do
    ~H"""
    <div
      id="upkeep-inspector"
      class={[
        "grid h-screen min-h-[720px] grid-rows-[56px_minmax(420px,1fr)_240px] overflow-hidden bg-neutral-50 text-[13px] text-neutral-950 antialiased",
        "[font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe_UI',system-ui,sans-serif]",
        "max-[1100px]:h-auto max-[1100px]:min-h-screen max-[1100px]:overflow-visible max-[1100px]:grid-rows-[56px_minmax(0,1fr)]"
      ]}
    >
      <.header document={@upkeep_document} />

      <main class="grid min-h-0 grid-cols-[320px_minmax(520px,1fr)_360px] overflow-hidden max-[1100px]:grid-cols-1">
        <Panels.playground_panel document={@upkeep_document} />
        <GraphComponent.graph_panel document={@upkeep_document} layout={@upkeep_layout} />
        <Panels.node_inspector_panel
          activity={@upkeep_activity}
          layout={@upkeep_layout}
          snapshot={@upkeep_snapshot}
        />
      </main>

      <section class="grid min-h-0 grid-cols-[minmax(0,1fr)_minmax(0,1fr)] border-t border-neutral-200 bg-white max-[1100px]:grid-cols-1">
        <Panels.timeline_panel timeline={@upkeep_timeline} />
        <Panels.code_panel code={@upkeep_code} />
      </section>
    </div>
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
          {@document.live_view.view_label || "Phoenix LiveView runtime"}
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
end
