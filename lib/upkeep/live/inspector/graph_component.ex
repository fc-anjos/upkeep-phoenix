defmodule Upkeep.Live.Inspector.GraphComponent do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Live.Inspector.Format

  attr :document, :map, required: true
  attr :layout, :map, required: true

  def graph_panel(assigns) do
    ~H"""
    <section
      id="upkeep-dag-panel"
      class={[
        "relative min-h-0 overflow-hidden bg-white",
        "bg-[linear-gradient(#f5f5f5_1px,transparent_1px),linear-gradient(90deg,#f5f5f5_1px,transparent_1px)] [background-size:24px_24px]"
      ]}
    >
      <div class="pointer-events-none absolute inset-x-3 top-3 z-10 flex items-center gap-2">
        <div class="pointer-events-auto flex items-center gap-3 rounded-md border border-neutral-200 bg-white/90 px-2.5 py-1.5 font-mono text-[11px] text-neutral-600 shadow-sm">
          <span class="inline-flex items-center gap-1.5">
            <span class="h-2.5 w-2.5 rounded-sm border border-[oklch(0.55_0.15_250)] bg-[oklch(0.95_0.04_250)]">
            </span>
            changed root
          </span>
          <span class="inline-flex items-center gap-1.5">
            <span class="h-2.5 w-2.5 rounded-sm border border-[oklch(0.68_0.15_70)] bg-[oklch(0.96_0.05_70)]">
            </span>
            recompute
          </span>
          <span class="inline-flex items-center gap-1.5">
            <span class="h-2.5 w-2.5 rounded-sm border border-[oklch(0.6_0.15_145)] bg-[oklch(0.95_0.05_145)]">
            </span>
            changed
          </span>
          <span class="inline-flex items-center gap-1.5">
            <span class="h-2.5 w-2.5 rounded-sm border border-neutral-300 bg-neutral-100"></span>
            cold/skipped
          </span>
        </div>

        <div class="pointer-events-auto ml-auto flex items-center gap-3 rounded-md border border-neutral-200 bg-white/90 px-2.5 py-1.5 font-mono text-[11px] text-neutral-700 shadow-sm">
          <span>{length(@document.dag.nodes)} nodes</span>
          <span>{length(@document.dag.edges)} edges</span>
          <span>{length(@document.events)} events</span>
        </div>
      </div>

      <div class="h-full overflow-auto px-6 pb-6 pt-14">
        <div class="flex min-h-full min-w-full items-center justify-center">
          <svg
            id="upkeep-dag-svg"
            class="block"
            width={@layout.width}
            height={@layout.height}
            viewBox={"0 0 #{@layout.width} #{@layout.height}"}
            role="img"
            aria-label="Upkeep DAG visualization"
          >
            <defs>
              <marker
                id="upkeep-arrow"
                viewBox="0 0 10 10"
                refX="9"
                refY="5"
                markerWidth="7"
                markerHeight="7"
                orient="auto-start-reverse"
              >
                <path d="M0,0 L10,5 L0,10 z" fill="#a3a3a3" />
              </marker>
              <marker
                id="upkeep-arrow-active"
                viewBox="0 0 10 10"
                refX="9"
                refY="5"
                markerWidth="7"
                markerHeight="7"
                orient="auto-start-reverse"
              >
                <path d="M0,0 L10,5 L0,10 z" fill="oklch(0.55 0.15 250)" />
              </marker>
              <pattern
                id="upkeep-hatch"
                patternUnits="userSpaceOnUse"
                width="6"
                height="6"
                patternTransform="rotate(45)"
              >
                <rect width="6" height="6" fill="#f5f5f5" />
                <line x1="0" y1="0" x2="0" y2="6" stroke="#cfcfcf" stroke-width="1.5" />
              </pattern>
            </defs>

            <path
              :for={edge <- @layout.edges}
              class="transition-[opacity,stroke] duration-150"
              d={edge.path}
              fill="none"
              stroke={edge.stroke}
              stroke-width={edge.width}
              stroke-opacity={edge.opacity}
              stroke-dasharray={edge.dash}
              marker-end={edge.marker}
            />

            <a
              :for={node <- @layout.nodes}
              href={"##{node.inspect_dom_id}"}
              class="group outline-none"
            >
              <.svg_node node={node} />
            </a>
          </svg>
        </div>
      </div>
    </section>
    """
  end

  attr :node, :map, required: true

  defp svg_node(assigns) do
    ~H"""
    <g id={@node.dom_id} transform={"translate(#{@node.x}, #{@node.y})"}>
      <%= if @node.kind == :source do %>
        <rect
          class={[
            "transition-[fill,stroke,stroke-width] duration-150 group-hover:stroke-2",
            @node.pulse_class
          ]}
          x="0"
          y="0"
          width={@node.width}
          height={@node.height}
          rx="4"
          fill={@node.fill}
          stroke={@node.stroke}
          stroke-width="1.25"
        />
        <rect
          x="0"
          y="0"
          width="4"
          height={@node.height}
          rx="2"
          fill={@node.stroke}
          opacity="0.7"
        />
      <% else %>
        <rect
          class={[
            "transition-[fill,stroke,stroke-width] duration-150 group-hover:stroke-2",
            @node.pulse_class
          ]}
          x="0"
          y="0"
          width={@node.width}
          height={@node.height}
          rx={if(@node.kind == :derived, do: div(@node.height, 2), else: 6)}
          fill={@node.fill}
          stroke={@node.stroke}
          stroke-width="1.25"
          stroke-dasharray={if(@node.kind == :component, do: "6 3", else: nil)}
        />
        <%= if @node.kind == :component do %>
          <rect
            x="0"
            y="0"
            width={@node.width}
            height="14"
            rx="6"
            fill={@node.stroke}
            opacity="0.18"
          />
          <line
            x1="0"
            y1="14"
            x2={@node.width}
            y2="14"
            stroke={@node.stroke}
            stroke-opacity="0.4"
            stroke-width="0.75"
          />
        <% end %>
      <% end %>

      <text
        x={if(@node.kind == :source, do: 13, else: 14)}
        y={if(@node.kind == :component, do: 29, else: div(@node.height, 2) + 4)}
        fill="#0a0a0a"
        font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
        font-size="12"
        font-weight="700"
      >
        {Format.truncate(@node.short_label, 22)}
      </text>
      <text
        x={@node.width - 14}
        y={@node.height - 9}
        fill="#737373"
        font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
        font-size="9"
        text-anchor="end"
      >
        {@node.kind_code}
      </text>
      <%= if @node.scope in [:shared, :local] do %>
        <g transform={"translate(#{@node.width - 18}, #{if(@node.kind == :component, do: 20, else: 10)})"}>
          <circle
            r="7"
            fill={scope_fill(@node.scope)}
            stroke={scope_stroke(@node.scope)}
            stroke-width="1"
          />
          <text
            x="0"
            y="3"
            fill={scope_text(@node.scope)}
            font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
            font-size="8"
            font-weight="700"
            text-anchor="middle"
          >
            {scope_initial(@node.scope)}
          </text>
        </g>
      <% end %>
      <%= if Map.has_key?(@node, :step) do %>
        <g transform={"translate(-12, #{div(@node.height, 2)})"}>
          <circle r="11" fill="#ffffff" stroke="oklch(0.68 0.15 70)" stroke-width="1.5" />
          <text
            x="0"
            y="4"
            fill="oklch(0.45 0.15 70)"
            font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
            font-size="11"
            font-weight="700"
            text-anchor="middle"
          >
            {@node.step}
          </text>
        </g>
      <% end %>
    </g>
    """
  end

  defp scope_fill(:shared), do: "oklch(0.95 0.03 195)"
  defp scope_fill(:local), do: "oklch(0.95 0.04 35)"

  defp scope_stroke(:shared), do: "oklch(0.6 0.12 195)"
  defp scope_stroke(:local), do: "oklch(0.62 0.13 35)"

  defp scope_text(:shared), do: "oklch(0.42 0.13 195)"
  defp scope_text(:local), do: "oklch(0.45 0.13 35)"

  defp scope_initial(:shared), do: "S"
  defp scope_initial(:local), do: "L"
end
