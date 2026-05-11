defmodule Upkeep.DemoAppWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :upkeep

  @session_options [
    store: :cookie,
    key: "_upkeep_demo_app_key",
    signing_salt: "demo-app",
    same_site: "Lax"
  ]

  socket "/live", Phoenix.LiveView.Socket,
    websocket: [connect_info: [session: @session_options]],
    longpoll: false

  plug Plug.Session, @session_options
  plug Upkeep.DemoAppWeb.Router
end
