import Config

# For development, we disable any cache and enable
# debugging and code reloading.
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  http: [ip: {0, 0, 0, 0}, port: 4000],
  check_origin: false,
  code_reloader: true,
  debug_errors: true,
  secret_key_base: "kGXdBPu7hzRdGVu4OuBhL+Gp6T3NUQxRt5BqzVgJ0fVzYKHYIEKcL3vFRbZhfGtl",
  watchers: [
    node: ["build.js", "--watch", cd: Path.expand("../assets", __DIR__)],
    # esbuild: {Esbuild, :install_and_run, [:default, ~w(--sourcemap=inline --watch)]},
    tailwind: {Tailwind, :install_and_run, [:default, ~w(--watch)]}
  ]

# Watch static and templates for browser reloading
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  live_reload: [
    patterns: [
      ~r"priv/static/.*(js|css|png|jpeg|jpg|gif|svg)$",
      ~r"priv/gettext/.*(po)$",
      ~r"lib/kafkaesque_dashboard/(controllers|live|components)/.*(ex|heex)$"
    ]
  ]

# Enable dev routes for dashboard in development
config :kafkaesque_dashboard, dev_routes: true
