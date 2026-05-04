ExUnit.start()
{:ok, _pid} = Upkeep.Repo.start_link()
Ecto.Adapters.SQL.Sandbox.mode(Upkeep.Repo, :manual)
