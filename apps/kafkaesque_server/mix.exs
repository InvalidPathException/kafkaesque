defmodule KafkaesqueServer.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafkaesque_server,
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
      mod: {KafkaesqueServer.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:kafkaesque_core, in_umbrella: true},
      {:phoenix, "~> 1.7"},
      {:grpc, "~> 0.7"},
      {:protobuf, "~> 0.12"},
      {:gun, "~> 2.0"},
      {:cowboy, "~> 2.10"},
      {:plug, "~> 1.15"},
      {:plug_cowboy, "~> 2.6"},
      {:cors_plug, "~> 3.0"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:telemetry_metrics, "~> 1.0"}
    ]
  end

  defp aliases do
    [
      test: ["test"]
    ]
  end
end