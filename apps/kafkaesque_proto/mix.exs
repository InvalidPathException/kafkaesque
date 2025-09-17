defmodule KafkaesqueProto.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafkaesque_proto,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:protobuf, "~> 0.12"},
      {:grpc, "~> 0.7"}
    ]
  end

  defp aliases do
    [
      "proto.gen": [
        "cmd protoc --elixir_out=plugins=grpc:./lib --proto_path=./priv/protos ./priv/protos/*.proto"
      ]
    ]
  end
end
