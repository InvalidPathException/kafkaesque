# Load shared test support modules from root
Code.require_file("../../../test/support/test_config.ex", __DIR__)
Code.require_file("../../../test/support/test_factory.ex", __DIR__)
Code.require_file("../../../test/support/test_client.ex", __DIR__)
Code.require_file("../../../test/support/test_helpers.ex", __DIR__)

# Configure test environment using shared config
Kafkaesque.Test.Config.setup_test_env()

# Clean up any previous test runs
Kafkaesque.Test.Config.cleanup_test_dirs()

# Ensure test directories exist
Kafkaesque.Test.Config.ensure_test_dirs()

# Start required applications in order
{:ok, _} = Application.ensure_all_started(:telemetry)
{:ok, _} = Application.ensure_all_started(:phoenix_pubsub)

# Start core application (includes Registry and TopicSupervisor)
{:ok, _} = Application.ensure_all_started(:kafkaesque_core)

# Start the server application with servers enabled for integration tests
{:ok, _} = Application.ensure_all_started(:kafkaesque_server)

# Wait for HTTP server to be ready
Kafkaesque.Test.Config.wait_for_http_server()

# Start ExUnit with proper configuration
ExUnit.start(
  capture_log: true,
  max_cases: System.schedulers_online() * 2,
  exclude: [:skip]
)
