defmodule UpkeepWeb.LiveIntegrationContractTest do
  use UpkeepWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  defmodule Counter do
    defstruct [:id]
  end

  defmodule CounterSource do
    use Upkeep.Source

    query(fn s ->
      UpkeepWeb.LiveIntegrationContractTest.table_value({:counter, s.id})
    end)

    invalidated_by(Counter, :updated, on: :id)
  end

  defmodule ClientIslandLive do
    use Phoenix.LiveView
    use Upkeep.Live

    @impl true
    def mount(_params, _session, socket) do
      {:ok, watch(socket, :counter, CounterSource, id: 1)}
    end

    @impl true
    def render(assigns) do
      ~H"""
      <main>
        <p id="server-counter">{@counter}</p>
        <section id="client-owned-island" phx-update="ignore">
          <span id="client-owned-value">client-owned</span>
        </section>
      </main>
      """
    end
  end

  setup do
    case :ets.whereis(__MODULE__) do
      :undefined -> :ok
      table -> :ets.delete(table)
    end

    table = :ets.new(__MODULE__, [:set, :public, :named_table])
    :ets.insert(table, {{:counter, 1}, "one"})

    on_exit(fn ->
      case :ets.whereis(__MODULE__) do
        :undefined -> :ok
        table -> :ets.delete(table)
      end
    end)

    :ok
  end

  test "watched assigns update next to a LiveView ignored client island", %{conn: conn} do
    {:ok, view, html} = live_isolated(conn, ClientIslandLive)

    assert html =~ ~s(id="server-counter")
    assert html =~ "one"
    assert html =~ ~s(id="client-owned-island" phx-update="ignore")
    assert html =~ ~s(id="client-owned-value")

    :ets.insert(__MODULE__, {{:counter, 1}, "two"})

    assert :ok =
             %Counter{id: 1}
             |> Upkeep.Change.updated()
             |> Upkeep.notify()

    html = assert_eventually_render(view, "two")

    assert html =~ ~s(id="client-owned-island" phx-update="ignore")
    assert html =~ ~s(id="client-owned-value")
    assert html =~ "client-owned"
  end

  def table_value(key) do
    [{^key, value}] = :ets.lookup(__MODULE__, key)
    value
  end

  defp assert_eventually_render(view, expected, attempts \\ 20)

  defp assert_eventually_render(view, expected, attempts) when attempts > 0 do
    html = render(view)

    if html =~ expected do
      html
    else
      sync_live_view(view)
      assert_eventually_render(view, expected, attempts - 1)
    end
  end

  defp assert_eventually_render(view, expected, 0) do
    html = render(view)
    flunk("expected rendered LiveView to include #{inspect(expected)}, got:\n#{html}")
  end

  defp sync_live_view(view) do
    :ok = Upkeep.Coordinator.Graph.drain()
    :sys.get_state(view.pid)
    :ok
  end
end
