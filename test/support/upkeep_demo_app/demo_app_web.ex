defmodule Upkeep.DemoAppWeb do
  @moduledoc false

  use Boundary,
    top_level?: true,
    check: [out: false]

  def static_paths, do: []

  def router do
    quote do
      use Phoenix.Router, helpers: false

      import Phoenix.LiveView.Router
    end
  end

  def live_view do
    quote do
      use Phoenix.LiveView, layout: false
      use Upkeep.Live

      unquote(html_helpers())
    end
  end

  def html do
    quote do
      use Phoenix.Component

      unquote(html_helpers())
    end
  end

  defp html_helpers do
    quote do
      alias Upkeep.DemoAppWeb.Layouts
    end
  end

  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
