ExUnit.start()
{node_root, 0} = System.cmd("mise", ["where", "node"], stderr_to_stdout: true)
node_bin = node_root |> String.trim() |> Path.join("bin")
System.put_env("PATH", node_bin <> ":" <> System.get_env("PATH", ""))

{:ok, _} = PhoenixTest.Playwright.Supervisor.start_link()
Application.put_env(:phoenix_test, :base_url, UpkeepWeb.Endpoint.url())
Ecto.Adapters.SQL.Sandbox.mode(Upkeep.Repo, :manual)
