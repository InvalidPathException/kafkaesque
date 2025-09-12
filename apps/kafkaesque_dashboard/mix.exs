defmodule KafkaesqueDashboard.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafkaesque_dashboard,
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
      mod: {KafkaesqueDashboard.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:kafkaesque_core, in_umbrella: true},
      {:phoenix, "~> 1.7"},
      {:phoenix_html, "~> 4.1"},
      {:phoenix_live_view, "~> 1.0"},
      {:phoenix_live_dashboard, "~> 0.8"},
      {:phoenix_pubsub, "~> 2.1"},
      {:phoenix_live_reload, "~> 1.5", only: :dev},
      {:live_svelte, "~> 0.16.0"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.1"},
      {:jason, "~> 1.4"},
      {:plug_cowboy, "~> 2.7"},
      {:heroicons, "~> 0.5"},
      {:floki, ">= 0.36.0", only: :test}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "assets.setup", "assets.build"],
      test: ["test"],
      "assets.setup": ["cmd --cd assets npm ci"],
      "assets.build": ["cmd --cd assets npm run build"],
      "assets.deploy": [
        "cmd --cd assets npm run build",
        "phx.digest"
      ]
    ]
  end
end
