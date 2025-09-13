import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :kafkaesque_dashboard, KafkaesqueDashboard.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "test_secret_key_base_at_least_64_chars_long_aaaaaaaaaaaaaaaaaaaaaaa",
  server: false
