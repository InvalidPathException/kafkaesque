defmodule KafkaesqueServer.TestSetup do
  @moduledoc """
  Centralized test setup for server tests.
  """

  def setup_integration_test do
    # Ensure HTTP endpoint is started for integration tests
    ensure_endpoint_started()

    # Ensure gRPC is available if needed
    ensure_grpc_started()

    :ok
  end

  defp ensure_endpoint_started do
    case Process.whereis(KafkaesqueServer.Endpoint) do
      nil ->
        {:ok, _} = KafkaesqueServer.Endpoint.start_link()
      _pid ->
        :ok
    end
  end

  defp ensure_grpc_started do
    # Check if GRPC endpoint needs to be started
    if Code.ensure_loaded?(GRPC.Server) do
      # Check if already running
      case Process.whereis(KafkaesqueServer.GRPC.Endpoint) do
        nil ->
          # Not running, try to start
          case GRPC.Server.start_endpoint(KafkaesqueServer.GRPC.Endpoint, 50_052) do
            {:ok, _pid, _port} -> :ok
            {:error, {:already_started, _}} -> :ok
            {:error, :eaddrinuse} ->
              # Port already in use, that's ok
              :ok
            error ->
              IO.warn("Failed to start gRPC: #{inspect(error)}")
              :ok
          end
        _pid ->
          # Already running
          :ok
      end
    else
      :ok
    end
  end
end
