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
        "relative h-full min-h-0 overflow-hidden bg-white",
        "bg-[linear-gradient(#f5f5f5_1px,transparent_1px),linear-gradient(90deg,#f5f5f5_1px,transparent_1px)] [background-size:24px_24px]"
      ]}
    >
      <div class="pointer-events-none absolute inset-x-0 top-0 z-10 flex items-baseline justify-between gap-3 border-b border-neutral-200 bg-white/85 px-5 py-2 backdrop-blur-sm">
        <h2 class="pointer-events-auto m-0 text-base font-semibold leading-tight text-neutral-950">
          DAG
          <span class="ml-2 font-mono text-xs font-normal text-neutral-500">
            {length(@document.dag.nodes)} nodes · {length(@document.dag.edges)} edges
          </span>
        </h2>
        <div class="pointer-events-auto flex items-center gap-4 font-mono text-xs text-neutral-600">
          <span class="inline-flex items-center gap-1.5">
            <span class="h-2 w-2 rounded-full bg-blue-600"></span> active
          </span>
          <span class="inline-flex items-center gap-1.5">
            <span class="h-2 w-2 rounded-full border border-neutral-300 bg-white"></span> idle
          </span>
        </div>
      </div>

      <div class="h-full w-full overflow-auto px-6 pb-6 pt-12">
        <div class="flex h-full min-h-full w-full min-w-full items-center justify-center">
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
                <path d="M0,0 L10,5 L0,10 z" fill="#2563eb" />
              </marker>
            </defs>

            <path
              :for={edge <- @layout.edges}
              d={edge.path}
              fill="none"
              stroke={edge.stroke}
              stroke-width={edge.width}
              marker-end={if(edge.active?, do: "url(#upkeep-arrow-active)", else: "url(#upkeep-arrow)")}
            />

            <a
              :for={node <- @layout.nodes}
              href={"##{node.inspect_dom_id}"}
              class="outline-none"
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
      <rect
        x="0"
        y="0"
        width={@node.width}
        height={@node.height}
        rx={node_radius(@node.kind, @node.height)}
        fill={@node.fill}
        stroke={@node.stroke}
        stroke-width="1.25"
        stroke-dasharray={if(@node.kind == :component, do: "4 3", else: nil)}
      />

      <text
        x="14"
        y={div(@node.height, 2) + 4}
        fill="#0a0a0a"
        font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
        font-size="12"
        font-weight="600"
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
        <g transform={"translate(#{@node.width - 18}, 10)"}>
          <circle r="7" fill="#fafafa" stroke="#a3a3a3" stroke-width="1" />
          <text
            x="0"
            y="3"
            fill="#404040"
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
          <circle r="11" fill="#ffffff" stroke="#2563eb" stroke-width="1.5" />
          <text
            x="0"
            y="4"
            fill="#1d4ed8"
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

  defp node_radius(:derived, height), do: div(height, 2)
  defp node_radius(_, _), do: 6

  defp scope_initial(:shared), do: "S"
  defp scope_initial(:local), do: "L"
end
