defmodule KafkaesqueServer.HealthController do
  @moduledoc """
  Health check endpoint for monitoring and load balancers.
  """

  use Phoenix.Controller, formats: [:json]
  require Logger

  alias Kafkaesque.Health.Monitor

  @doc """
  Health check endpoint that returns system health status.
  Returns 200 OK when healthy, 503 Service Unavailable when unhealthy.
  """
  def check(conn, _params) do
    case Monitor.status() do
      :healthy ->
        json(conn, %{
          status: "healthy",
          service: "kafkaesque",
          timestamp: System.system_time(:millisecond)
        })

      :degraded ->
        conn
        |> put_status(200)
        |> json(%{
          status: "degraded",
          service: "kafkaesque",
          timestamp: System.system_time(:millisecond),
          message: "Service is operational but degraded"
        })

      :unhealthy ->
        conn
        |> put_status(503)
        |> json(%{
          status: "unhealthy",
          service: "kafkaesque",
          timestamp: System.system_time(:millisecond),
          message: "Service is unhealthy"
        })

      {:error, :monitor_not_running} ->
        Logger.error("Health monitor is not running")
        conn
        |> put_status(503)
        |> json(%{
          status: "unknown",
          service: "kafkaesque",
          timestamp: System.system_time(:millisecond),
          message: "Health monitor not available"
        })
    end
  end

  @doc """
  Detailed health information endpoint.
  Returns comprehensive health metrics and check results.
  """
  def detailed(conn, _params) do
    case Monitor.get_health_info() do
      {:error, :monitor_not_running} ->
        conn
        |> put_status(503)
        |> json(%{
          error: "Health monitor not available"
        })

      info ->
        json(conn, %{
          status: info.status,
          last_check: info.last_check,
          checks: format_checks(info.checks),
          failures: info.failures,
          service: "kafkaesque",
          timestamp: System.system_time(:millisecond)
        })
    end
  end

  defp format_checks(checks) do
    Enum.map(checks, fn {name, result} ->
      case result do
        {:ok, data} ->
          %{
            name: name,
            status: "healthy",
            data: data
          }

        {:warning, data} ->
          %{
            name: name,
            status: "degraded",
            data: data
          }

        {:error, message} ->
          %{
            name: name,
            status: "unhealthy",
            error: to_string(message)
          }
      end
    end)
  end
end
