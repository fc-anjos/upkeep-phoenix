defmodule Upkeep.DemoAppWeb.Router do
  use Upkeep.DemoAppWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_secure_browser_headers
  end

  scope "/", Upkeep.DemoAppWeb do
    pipe_through :browser

    live_session :demo_scope, on_mount: Upkeep.DemoAppWeb.CurrentScope do
      live "/notes", NotesLive, :index
    end
  end
end
