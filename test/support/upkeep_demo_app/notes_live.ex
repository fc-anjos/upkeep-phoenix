defmodule Upkeep.DemoAppWeb.NotesLive do
  @moduledoc false

  use Upkeep.DemoAppWeb, :live_view

  alias Upkeep.DemoApp.VisibleNotes

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> watch(:notes, VisibleNotes, %{
        user_id: socket.assigns.current_scope.user_id
      })
      |> derive(:note, [:notes], &__MODULE__.visible_note/1)

    {:ok, socket}
  end

  def visible_note(%{notes: [note]}), do: note
  def visible_note(%{notes: []}), do: nil

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash} current_scope={@current_scope}>
      <section id="notes-panel">
        <h1 id="notes-title">Notes</h1>

        <div id="visible-notes">
          <article :if={@note} id={"note-#{@note.id}"} data-note-id={@note.id}>
            {@note.title}
          </article>
        </div>
      </section>
    </Layouts.app>
    """
  end
end
