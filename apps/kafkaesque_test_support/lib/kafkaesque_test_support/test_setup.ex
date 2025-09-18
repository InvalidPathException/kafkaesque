defmodule KafkaesqueTestSupport.TestSetup do
  @moduledoc """
  Centralized test setup for integration tests.
  """

  def setup_integration_test do
    # Ensure all required applications are started
    Application.ensure_all_started(:kafkaesque_core)

    # Only start server components if they're available
    if Code.ensure_loaded?(:"Elixir.KafkaesqueServer.Application") do
      Application.ensure_all_started(:kafkaesque_server)
    end

    Application.ensure_all_started(:grpc)

    # Start the gRPC endpoint
    ensure_grpc_endpoint_started()

    :ok
  end

  defp ensure_grpc_endpoint_started do
    # Use atoms to avoid compile-time dependency on KafkaesqueServer
    endpoint_module = :"Elixir.KafkaesqueServer.GRPC.Endpoint"

    if Code.ensure_loaded?(endpoint_module) and Code.ensure_loaded?(GRPC.Server) do
      # First check if it's already running
      case Process.whereis(endpoint_module) do
        nil ->
          # Not running, start it
          # The endpoint needs to be defined first
          ensure_endpoint_defined()

          # Now start the endpoint on test port
          case GRPC.Server.start_endpoint(endpoint_module, 50_052) do
            {:ok, _pid, port} ->
              IO.puts("Started gRPC server on port #{port}")
              :ok
            {:error, {:already_started, _pid}} ->
              IO.puts("gRPC server already started")
              :ok
            {:error, :eaddrinuse} ->
              IO.puts("Port 50_052 already in use")
              :ok
            error ->
              IO.warn("Failed to start gRPC endpoint: #{inspect(error)}")
              :ok
          end
        _pid ->
          IO.puts("gRPC server already running")
          :ok
      end
    else
      IO.warn("gRPC modules not available")
      :ok
    end
  end

  defp ensure_endpoint_defined do
    # Define the endpoint if it doesn't exist
    endpoint_module = :"Elixir.KafkaesqueServer.GRPC.Endpoint"

    unless function_exported?(endpoint_module, :__info__, 1) do
      # Define the endpoint module dynamically
      # This matches what's in kafkaesque_server/lib/kafkaesque_server/grpc/endpoint.ex
      defmodule :"Elixir.KafkaesqueServer.GRPC.Endpoint" do
        use GRPC.Endpoint

        intercept GRPC.Server.Interceptors.Logger
        run Kafkaesque.GRPC.Service
      end
    end
  end
end
