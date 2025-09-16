# Set up test environment
Application.put_env(:kafkaesque_core, :data_dir, "/tmp/kafkaesque_test/data")
Application.put_env(:kafkaesque_core, :offsets_dir, "/tmp/kafkaesque_test/offsets")

# Configure faster batching for tests
# NOTE: All timeout values are in milliseconds for consistency
Application.put_env(:kafkaesque_core, :batch_size, 5)  # Smaller batches
Application.put_env(:kafkaesque_core, :batch_timeout, 100)  # 100ms for faster tests
Application.put_env(:kafkaesque_core, :fsync_interval_ms, 50)  # Faster fsync

# Clean up test directories before running tests
File.rm_rf!("/tmp/kafkaesque_test")
File.mkdir_p!("/tmp/kafkaesque_test/data")
File.mkdir_p!("/tmp/kafkaesque_test/offsets")

# Start the application to ensure all services are available
{:ok, _} = Application.ensure_all_started(:kafkaesque_core)

# Verify that critical processes are running
# Wait for Topic.Supervisor to be available
Process.sleep(100)

# Ensure Topic Supervisor is running
if Process.whereis(Kafkaesque.Topic.Supervisor) == nil do
  raise "Topic.Supervisor not started. Check the application supervisor configuration."
end

# Start :inets for HTTP client functionality
:inets.start()

# Test support files are automatically compiled from test/support/ directory
# via elixirc_paths configuration in mix.exs

ExUnit.start()
