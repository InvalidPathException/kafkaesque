defmodule KafkaesqueClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafkaesque_client,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:grpc, "~> 0.7"},
      {:protobuf, "~> 0.12"},
      {:gun, "~> 2.0"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:retry, "~> 0.18"},
      {:stream_data, "~> 1.0", only: :test}
    ]
  end

  defp aliases do
    [
      test: ["test"]
    ]
  end
end