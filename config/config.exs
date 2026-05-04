import Config

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

env_config = Path.expand("#{config_env()}.exs", __DIR__)

if File.exists?(env_config) do
  import_config "#{config_env()}.exs"
end
