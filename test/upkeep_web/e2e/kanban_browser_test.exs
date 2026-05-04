defmodule UpkeepWeb.KanbanBrowserTest do
  use PhoenixTest.Playwright.Case, async: false

  import PhoenixTest

  setup do
    Upkeep.Kanban.reset!()
    Upkeep.clear_events()
    :ok
  end

  test "browser-owned ignored island survives LiveView patches", %{conn: conn} do
    conn
    |> visit("/kanban")
    |> PhoenixTest.Playwright.evaluate(
      """
      new Promise((resolve) => {
        const deadline = Date.now() + 3000;
        const check = () => {
          const island = document.querySelector("#client-owned-kanban-state");

          if (island?.dataset.hookMounted === "true") {
            resolve(true);
          } else if (Date.now() > deadline) {
            resolve(false);
          } else {
            setTimeout(check, 50);
          }
        };

        check();
      })
      """,
      fn mounted? -> assert mounted? end
    )
    |> PhoenixTest.Playwright.evaluate("""
    document
      .querySelector("#client-owned-kanban-state [data-client-owned-value]")
      .textContent = "browser-owned";
    """)
    |> PhoenixTest.Playwright.type("input[name='issue[title]']", "Browser patched issue")
    |> click_button("Add")
    |> assert_has("#issue-100", text: "Browser patched issue")
    |> PhoenixTest.Playwright.evaluate(
      """
      document
        .querySelector("#client-owned-kanban-state [data-client-owned-value]")
        .textContent
      """,
      fn value -> assert value == "browser-owned" end
    )
  end
end
