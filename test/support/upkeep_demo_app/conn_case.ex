defmodule Upkeep.DemoAppWeb.ConnCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Phoenix.{ConnTest, PubSub}
  alias Plug.Test, as: PlugTest
  alias Upkeep.DemoApp
  alias Upkeep.DemoAppWeb.Endpoint
  alias Upkeep.TestSupport.DataCase

  using do
    quote do
      @endpoint Upkeep.DemoAppWeb.Endpoint

      import Phoenix.ConnTest
      import Phoenix.LiveViewTest
      import Plug.Conn
      import Upkeep.DemoAppWeb.ConnCase
    end
  end

  setup tags do
    DataCase.setup_sandbox(tags)

    start_supervised!({PubSub, name: DemoApp.PubSub})
    start_supervised!(Endpoint)

    {:ok, conn: ConnTest.build_conn()}
  end

  def signed_in_conn(conn, user_id) when is_integer(user_id) do
    PlugTest.init_test_session(conn, %{"user_id" => user_id})
  end
end
