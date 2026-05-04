ExUnit.start(exclude: [:multi_node])
{:ok, _pid} = Upkeep.Repo.start_link()
Upkeep.TestSchema.reset!()
Ecto.Adapters.SQL.Sandbox.mode(Upkeep.Repo, :manual)
