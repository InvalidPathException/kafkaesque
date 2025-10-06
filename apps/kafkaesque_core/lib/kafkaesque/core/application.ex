defmodule Kafkaesque.Core.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Kafkaesque.TopicRegistry},
      Kafkaesque.Topic.Metadata,
      Kafkaesque.Partition.Router,
      Kafkaesque.Topic.Supervisor,
      {Phoenix.PubSub, name: Kafkaesque.PubSub, adapter: Phoenix.PubSub.PG2},
      Kafkaesque.Telemetry.Supervisor,
      Kafkaesque.Telemetry,
      Kafkaesque.Health.Monitor,
      Kafkaesque.Workers.PoolSupervisor
    ]

    opts = [strategy: :one_for_one, name: Kafkaesque.Core.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
