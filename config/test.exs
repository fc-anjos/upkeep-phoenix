import Config

config :upkeep,
  ecto_repos: [Upkeep.TestSupport.Repo],
  repo: Upkeep.TestSupport.Repo,
  graph_retry: [base_delay_ms: 0, max_delay_ms: 0]

{test_adapter, repo_config} =
  case System.get_env("UPKEEP_TEST_ADAPTER") do
    "postgres" ->
      {Ecto.Adapters.Postgres,
       [
         database: System.get_env("UPKEEP_TEST_DATABASE", "upkeep_test"),
         username: System.get_env("PGUSER", System.get_env("USER", "postgres")),
         password: System.get_env("PGPASSWORD"),
         hostname: System.get_env("PGHOST", "localhost"),
         port: String.to_integer(System.get_env("PGPORT", "5432"))
       ]}

    _sqlite ->
      {Ecto.Adapters.SQLite3,
       [
         database: Path.expand("../upkeep_test.db", __DIR__),
         journal_mode: :wal,
         busy_timeout: 30_000,
         default_transaction_mode: :immediate
       ]}
  end

config :upkeep, :test_adapter, test_adapter

config :upkeep,
       Upkeep.TestSupport.Repo,
       Keyword.merge(repo_config,
         telemetry_prefix: [:upkeep, :repo],
         pool_size: 5,
         pool: Ecto.Adapters.SQL.Sandbox
       )

config :upkeep, Upkeep.DemoAppWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "Wh3w5xprv+UiSvmISWxUEZNWPi69+IJ5um+emjdczl18hS42SlBzyyLWa0osd0FJ",
  live_view: [signing_salt: "demo-app"],
  pubsub_server: Upkeep.DemoApp.PubSub,
  render_errors: [
    formats: [html: Upkeep.DemoAppWeb.ErrorHTML],
    layout: false
  ],
  server: false

config :logger, level: :warning

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  enable_expensive_runtime_checks: true
