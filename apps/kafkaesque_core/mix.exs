defmodule KafkaesqueCore.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafkaesque_core,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Kafkaesque.Core.Application, []},
      extra_applications: [:logger, :runtime_tools, :os_mon]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:gen_stage, "~> 1.2"},
      {:telemetry, "~> 1.2"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.0"},
      {:phoenix_pubsub, "~> 2.1"},
      {:jason, "~> 1.4"},
      {:nimble_options, "~> 1.1"},
      {:typed_struct, "~> 0.3"},
      {:poolboy, "~> 1.5"},
      {:stream_data, "~> 1.0", only: :test},
      {:benchee, "~> 1.3", only: :dev}
    ]
  end

  defp aliases do
    [
      test: ["test"]
    ]
  end
end
