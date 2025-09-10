import Config

# For development, disable request forgery protection
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4000],
  check_origin: false,
  code_reloader: true,
  debug_errors: true,
  secret_key_base: "IKaDjhEX2OxQvhzOYP3HhKqPcnL8vr2Ov3zKv6kCXy4vNPgA7qNXyxuE3zlZFWQe",
  watchers: []

config :kafkaesque_server, KafkaesqueServer.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4001],
  check_origin: false,
  code_reloader: true,
  debug_errors: true

# Configure LiveReload for development
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  live_reload: [
    patterns: [
      ~r"priv/static/(?!uploads/).*(js|css|png|jpeg|jpg|gif|svg)$",
      ~r"lib/kafkaesque_dashboard/(controllers|live|components)/.*(ex|heex)$"
    ]
  ]

# Core development settings
config :kafkaesque_core,
  data_dir: "./data",
  offsets_dir: "./offsets"

# Enable dev routes for dashboard
config :kafkaesque_dashboard, dev_routes: true

# Disable swoosh API client
config :swoosh, :api_client, false

# Set higher stacktrace depth
config :phoenix, :stacktrace_depth, 20

# Initialize plugs at runtime
config :phoenix, :plug_init_mode, :runtime

# Configure Elixir's logger
config :logger, :console, format: "[$level] $message\n"
