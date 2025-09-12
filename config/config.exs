import Config

# Configure Phoenix endpoint
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  url: [host: "localhost"],
  render_errors: [
    formats: [html: KafkaesqueDashboard.ErrorHTML, json: KafkaesqueDashboard.ErrorJSON],
    layout: false
  ],
  pubsub_server: Kafkaesque.PubSub,
  live_view: [signing_salt: "sRqyXF9z"]

# Configure server endpoint
config :kafkaesque_server, KafkaesqueServer.Endpoint,
  url: [host: "localhost"],
  adapter: Phoenix.Endpoint.Cowboy2Adapter,
  render_errors: [
    formats: [json: KafkaesqueServer.ErrorJSON],
    layout: false
  ]

# Core configuration
config :kafkaesque_core,
  data_dir: "/var/lib/kafkaesque/data",
  offsets_dir: "/var/lib/kafkaesque/offsets",
  retention_hours: 168,
  max_batch_size: 500,
  batch_timeout: 5,
  fsync_interval_ms: 1000

# Telemetry configuration
config :kafkaesque_core, :telemetry,
  enabled: true,
  poller_measurements: [
    {Kafkaesque.Telemetry, :dispatch_metrics, []}
  ]

# Phoenix configuration
config :phoenix, :json_library, Jason

# LiveSvelte configuration
config :live_svelte,
  ssr: false

# Logger configuration
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Import environment specific config
import_config "#{config_env()}.exs"
