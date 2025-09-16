defmodule Kafkaesque.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      listeners: [Phoenix.CodeReloader],
      elixirc_paths: elixirc_paths(Mix.env()),
      releases: [
        kafkaesque: [
          applications: [
            kafkaesque_core: :permanent,
            kafkaesque_server: :permanent,
            kafkaesque_dashboard: :permanent
          ]
        ]
      ],
      aliases: aliases()
    ]
  end

  defp elixirc_paths(:test), do: ["test/support"]
  defp elixirc_paths(_), do: []

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "deps.compile", "assets.setup"],
      test: ["test --no-start"],
      "format.check": ["format --check-formatted"],
      lint: ["compile --warnings-as-errors", "format.check", "credo --strict"],
      "proto.gen": [
        "cmd protoc --elixir_out=plugins=grpc:./apps/kafkaesque_server/lib/grpc --proto_path=./proto ./proto/kafkaesque.proto"
      ],
      "assets.setup": [
        "cmd --cd apps/kafkaesque_dashboard/assets npm install"
      ],
      "assets.build": [
        "cmd --cd apps/kafkaesque_dashboard/assets npm run build",
        "phx.digest"
      ],
      "assets.deploy": [
        "assets.build"
      ],
      "assets.watch": [
        "cmd --cd apps/kafkaesque_dashboard/assets npm run watch"
      ]
    ]
  end
end
