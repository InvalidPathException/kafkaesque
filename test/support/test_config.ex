defmodule Kafkaesque.Test.Config do
  @moduledoc """
  Centralized test configuration for all Kafkaesque apps.
  """

  # Test ports - fixed to avoid conflicts
  @http_port 4001
  @grpc_port 50_052
  @dashboard_port 4002

  # Test directories
  @test_root "/tmp/kafkaesque_test"

  def http_port, do: @http_port
  def grpc_port, do: @grpc_port
  def dashboard_port, do: @dashboard_port

  def test_data_dir do
    Path.join([@test_root, "#{System.unique_integer([:positive])}", "data"])
  end

  def test_offsets_dir do
    Path.join([@test_root, "#{System.unique_integer([:positive])}", "offsets"])
  end

  @doc """
  Configure all applications for testing.
  """
  def setup_test_env do
    # Core configuration - optimized for immediate writes in tests
    Application.put_env(:kafkaesque_core, :data_dir, test_data_dir())
    Application.put_env(:kafkaesque_core, :offsets_dir, test_offsets_dir())
    Application.put_env(:kafkaesque_core, :retention_hours, 1)
    Application.put_env(:kafkaesque_core, :max_batch_size, 10)
    # Immediate flush
    Application.put_env(:kafkaesque_core, :batch_timeout, 0)
    # Immediate sync
    Application.put_env(:kafkaesque_core, :fsync_interval_ms, 0)

    # Server configuration - enable server for integration tests
    Application.put_env(:kafkaesque_server, KafkaesqueServer.Endpoint,
      http: [port: @http_port],
      server: true
    )

    Application.put_env(:kafkaesque_server, :grpc_port, @grpc_port)

    # Dashboard configuration
    Application.put_env(:kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
      http: [port: @dashboard_port],
      secret_key_base: "test_secret_key_base_at_least_64_bytes_long_for_testing_purposes_only",
      server: false
    )

    # Logger configuration
    Application.put_env(:logger, :level, :warning)

    # Phoenix configuration
    Application.put_env(:phoenix, :plug_init_mode, :runtime)

    :ok
  end

  @doc """
  Clean up test directories.
  """
  def cleanup_test_dirs do
    File.rm_rf!(@test_root)
  end

  @doc """
  Ensure test directories exist.
  """
  def ensure_test_dirs do
    data_dir = test_data_dir()
    offsets_dir = test_offsets_dir()

    File.mkdir_p!(data_dir)
    File.mkdir_p!(offsets_dir)

    {data_dir, offsets_dir}
  end

  @doc """
  Wait for HTTP server to be ready.
  """
  def wait_for_http_server(retries \\ 30) do
    # Simple TCP connection check instead of HTTP request
    case :gen_tcp.connect(~c"localhost", @http_port, [:binary, active: false], 100) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        :ok

      {:error, _} when retries > 0 ->
        Process.sleep(100)
        wait_for_http_server(retries - 1)

      {:error, reason} ->
        IO.warn("HTTP server not ready after 3 seconds: #{inspect(reason)}")
        :ok
    end
  end
end
