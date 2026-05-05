defmodule Upkeep.TestSupport.Repo do
  use Upkeep.Ecto.Repo,
    otp_app: :upkeep,
    adapter: Ecto.Adapters.SQLite3
end
