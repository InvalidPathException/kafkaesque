import Config

# Configure endpoints for testing
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base:
    "test_secret_key_base_at_least_64_bytes_long_for_testing_purposes_only_not_for_production",
  server: false

config :kafkaesque_server, KafkaesqueServer.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4001],
  server: true

config :kafkaesque_server,
  start_grpc: false

# Core test settings - optimized for immediate writes
config :kafkaesque_core,
  data_dir: "/tmp/kafkaesque_test/data",
  offsets_dir: "/tmp/kafkaesque_test/offsets",
  retention_hours: 1,
  max_batch_size: 10,
  # Immediate flush in tests
  batch_timeout: 0,
  # Immediate fsync in tests
  fsync_interval_ms: 0

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime
config :phoenix, :plug_init_mode, :runtime
