defmodule KafkaesqueTestSupport.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafkaesque_test_support,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:kafkaesque_core, in_umbrella: true},
      {:grpc, "~> 0.7"}
    ]
  end
end