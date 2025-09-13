# Start the application to ensure all services are available
Application.ensure_all_started(:kafkaesque_core)

# Compile test support files
Code.require_file("support/test_case.ex", __DIR__)

ExUnit.start()
