# Unified test helper for all Kafkaesque apps
# This replaces the individual test_helper.exs files in each app

# Load test support modules
Code.require_file("test/support/test_config.ex")
Code.require_file("test/support/test_factory.ex")
Code.require_file("test/support/test_client.ex")
Code.require_file("test/support/test_helpers.ex")

# Configure test environment
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

# For server tests, start the server app but not the actual servers
Application.put_env(:kafkaesque_server, :start_servers, false)
{:ok, _} = Application.ensure_all_started(:kafkaesque_server)

# Start ExUnit with proper configuration
ExUnit.start(
  capture_log: true,
  max_cases: System.schedulers_online() * 2,
  exclude: [:skip]
)

# Add global test setup/teardown
ExUnit.after_suite(fn _results ->
  # Final cleanup after all tests
  Kafkaesque.Test.Config.cleanup_test_dirs()
end)
