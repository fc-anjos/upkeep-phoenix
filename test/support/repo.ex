defmodule Upkeep.TestSupport.Repo do
  use Upkeep.Ecto.Repo,
    otp_app: :upkeep,
    adapter: Application.compile_env(:upkeep, :test_adapter, Ecto.Adapters.SQLite3)
end
