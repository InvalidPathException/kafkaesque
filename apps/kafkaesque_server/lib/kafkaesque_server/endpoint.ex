defmodule KafkaesqueServer.Endpoint do
  use Phoenix.Endpoint, otp_app: :kafkaesque_server

  plug Plug.RequestId
  plug Plug.Telemetry, event_prefix: [:phoenix, :endpoint]

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head

  plug CORSPlug, origin: "*"

  # Health check endpoint
  plug KafkaesqueServer.HealthPlug

  plug KafkaesqueServer.Router
end
