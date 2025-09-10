import Config

# Runtime configuration for production deployments
if config_env() == :prod do
  # Core configuration
  config :kafkaesque_core,
    data_dir: System.get_env("DATA_DIR", "/var/lib/kafkaesque/data"),
    offsets_dir: System.get_env("OFFSETS_DIR", "/var/lib/kafkaesque/offsets"),
    retention_hours: String.to_integer(System.get_env("RETENTION_HOURS", "168")),
    max_batch_size: String.to_integer(System.get_env("MAX_BATCH_SIZE", "500")),
    batch_timeout: String.to_integer(System.get_env("BATCH_TIMEOUT", "5")),
    fsync_interval_ms: String.to_integer(System.get_env("FSYNC_INTERVAL_MS", "1000"))

  # Dashboard endpoint configuration
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  port = String.to_integer(System.get_env("PORT", "4000"))

  config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
    url: [host: System.get_env("PHX_HOST", "localhost"), port: port],
    http: [
      ip: {0, 0, 0, 0},
      port: port
    ],
    secret_key_base: secret_key_base

  # Server endpoint configuration
  config :kafkaesque_server, KafkaesqueServer.Endpoint,
    http: [
      ip: {0, 0, 0, 0},
      port: String.to_integer(System.get_env("API_PORT", "4001"))
    ]

  # gRPC configuration
  config :kafkaesque_server, :grpc, port: String.to_integer(System.get_env("GRPC_PORT", "50051"))

  # Logger configuration
  config :logger,
    level: String.to_atom(System.get_env("LOG_LEVEL", "info"))

  # Phoenix configuration
  config :phoenix,
    serve_endpoints: true

  # Telemetry configuration
  config :kafkaesque_core, :telemetry,
    enabled: System.get_env("TELEMETRY_ENABLED", "true") == "true"
end
