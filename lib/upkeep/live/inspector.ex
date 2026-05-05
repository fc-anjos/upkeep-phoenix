defmodule Upkeep.Live.Inspector do
  @moduledoc false

  use Phoenix.Component

  alias Upkeep.Introspection
  alias Upkeep.Live.Inspector.{Activity, Components, Layout, Presenter}

  def render(assigns) do
    document = Introspection.snapshot(assigns.socket, assigns: assigns)
    activity = Activity.from_document(document)
    layout = Layout.build(document.dag, activity)

    assigns
    |> assign(:upkeep_document, document)
    |> assign(:upkeep_activity, activity)
    |> assign(:upkeep_layout, layout)
    |> assign(:upkeep_timeline, Presenter.timeline_events(document.events))
    |> assign(:upkeep_code, Presenter.generated_declaration(document))
    |> assign(:upkeep_snapshot, Presenter.graph_snapshot(document))
    |> Components.page()
  end
end
