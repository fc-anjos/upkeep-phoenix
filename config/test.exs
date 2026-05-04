import Config

config :upkeep,
  ecto_repos: [Upkeep.Repo],
  repo: Upkeep.Repo

config :upkeep, Upkeep.Repo,
  database: Path.expand("../upkeep_test.db", __DIR__),
  pool_size: 5,
  pool: Ecto.Adapters.SQL.Sandbox

config :logger, level: :warning

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  enable_expensive_runtime_checks: true
