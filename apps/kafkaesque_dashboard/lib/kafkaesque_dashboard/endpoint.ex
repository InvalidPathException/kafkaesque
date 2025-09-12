defmodule KafkaesqueDashboard.Endpoint do
  use Phoenix.Endpoint, otp_app: :kafkaesque_dashboard

  @session_options [
    store: :cookie,
    key: "_kafkaesque_dashboard_key",
    signing_salt: "sRqyXF9z",
    same_site: "Lax"
  ]

  socket "/live", Phoenix.LiveView.Socket, websocket: [connect_info: [session: @session_options]]

  plug Plug.Static,
    at: "/",
    from: {:kafkaesque_dashboard, "priv/static"},
    gzip: false,
    only: ~w(assets fonts images favicon.ico robots.txt demo.html js css)

  if code_reloading? do
    socket "/phoenix/live_reload/socket", Phoenix.LiveReloader.Socket
    plug Phoenix.LiveReloader
    plug Phoenix.CodeReloader
  end

  plug Phoenix.LiveDashboard.RequestLogger,
    param_key: "request_logger",
    cookie_key: "request_logger"

  plug Plug.RequestId
  plug Plug.Telemetry, event_prefix: [:phoenix, :endpoint]

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head
  plug Plug.Session, @session_options
  plug KafkaesqueDashboard.Router
end
