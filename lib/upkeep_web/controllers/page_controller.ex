defmodule UpkeepWeb.PageController do
  use UpkeepWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
