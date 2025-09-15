defmodule KafkaesqueServer.HealthPlug do
  @moduledoc """
  Health check endpoint plug.
  Provides both liveness (/healthz) and readiness (/ready) checks.
  """

  import Plug.Conn
  alias Kafkaesque.Health

  def init(opts), do: opts

  def call(%Plug.Conn{path_info: ["healthz"]} = conn, _opts) do
    # Liveness check - simple check if system is alive
    # Health.alive?() always returns :ok, so we just use it directly
    Health.alive?()

    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "OK")
    |> halt()
  end

  def call(%Plug.Conn{path_info: ["ready"]} = conn, _opts) do
    # Readiness check - comprehensive health check
    case Health.check() do
      {:ok, health_info} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(health_info))
        |> halt()

      {:error, health_info} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(503, Jason.encode!(health_info))
        |> halt()
    end
  end

  def call(%Plug.Conn{path_info: ["health"]} = conn, _opts) do
    # Detailed health info endpoint
    info = Health.info()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(info))
    |> halt()
  end

  def call(conn, _opts), do: conn
end
