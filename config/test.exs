import Config

config :upkeep,
  ecto_repos: [Upkeep.TestSupport.Repo],
  repo: Upkeep.TestSupport.Repo,
  graph_retry: [base_delay_ms: 0, max_delay_ms: 0]

config :upkeep, Upkeep.TestSupport.Repo,
  database: Path.expand("../upkeep_test.db", __DIR__),
  telemetry_prefix: [:upkeep, :repo],
  pool_size: 5,
  pool: Ecto.Adapters.SQL.Sandbox

config :logger, level: :warning

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  enable_expensive_runtime_checks: true
