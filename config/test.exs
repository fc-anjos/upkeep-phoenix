import Config

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
config :upkeep, Upkeep.Repo,
  database: Path.expand("../upkeep_test.db", __DIR__),
  pool_size: 5,
  pool: Ecto.Adapters.SQL.Sandbox

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :upkeep, UpkeepWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "JYZXHlXGNmKj0icIjYP9lIiElYmE0yguKU9H650eTYx5Tzp6Sb69NiSlE9iDLYIO",
  server: true

config :phoenix_test,
  otp_app: :upkeep,
  playwright: [
    browser_pool: :chromium_pool,
    browser_pools: [
      [
        id: :chromium_pool,
        browser: :chromium,
        executable_path:
          System.get_env(
            "PLAYWRIGHT_CHROME_PATH",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
          )
      ]
    ],
    js_logger: false,
    timeout: 5_000,
    browser_launch_timeout: 15_000
  ]

# In test we don't send emails
config :upkeep, Upkeep.Mailer, adapter: Swoosh.Adapters.Test

# Disable swoosh api client as it is only required for production adapters
config :swoosh, :api_client, false

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Enable helpful, but potentially expensive runtime checks
config :phoenix_live_view,
  enable_expensive_runtime_checks: true

# Sort query params output of verified routes for robust url comparisons
config :phoenix,
  sort_verified_routes_query_params: true
