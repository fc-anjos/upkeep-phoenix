ExUnit.start(exclude: [:multi_node])
{:ok, _pid} = Upkeep.TestSupport.Repo.start_link()
Upkeep.TestSupport.Schema.reset!()
Ecto.Adapters.SQL.Sandbox.mode(Upkeep.TestSupport.Repo, :manual)
