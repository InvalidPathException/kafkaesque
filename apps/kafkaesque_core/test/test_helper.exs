# Start the application to ensure all services are available
Application.ensure_all_started(:kafkaesque_core)

# Test support files are automatically compiled from test/support/ directory
# via elixirc_paths configuration in mix.exs

ExUnit.start()
