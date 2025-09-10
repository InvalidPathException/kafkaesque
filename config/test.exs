import Config

# Configure endpoints for testing
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base:
    "test_secret_key_base_at_least_64_bytes_long_for_testing_purposes_only_not_for_production",
  server: false

config :kafkaesque_server, KafkaesqueServer.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4003],
  server: false

# Core test settings
config :kafkaesque_core,
  data_dir: "./test_data",
  offsets_dir: "./test_offsets",
  retention_hours: 1,
  max_batch_size: 10,
  batch_timeout: 1

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime
config :phoenix, :plug_init_mode, :runtime
