import Config

config :esbuild,
  version: "0.19.5",
  default: [
    args:
      ~w(js/app.js --bundle --target=es2017 --outdir=../priv/static/assets --external:/fonts/* --external:/images/*),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
  ]

config :tailwind,
  version: "3.3.5",
  default: [
    args: ~w(
      --config=tailwind.config.js
      --input=css/app.css
      --output=../priv/static/assets/app.css
    ),
    cd: Path.expand("../assets", __DIR__)
  ]

config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  url: [host: "localhost"],
  adapter: Phoenix.Endpoint.Cowboy2Adapter,
  render_errors: [
    formats: [html: KafkaesqueDashboard.ErrorHTML, json: KafkaesqueDashboard.ErrorJSON],
    layout: false
  ],
  pubsub_server: Kafkaesque.PubSub,
  live_view: [signing_salt: "7PLFcKsR"]

config :phoenix_live_dashboard,
  metrics: KafkaesqueDashboard.Telemetry

config :kafkaesque_dashboard, dev_routes: true

import_config "#{config_env()}.exs"
