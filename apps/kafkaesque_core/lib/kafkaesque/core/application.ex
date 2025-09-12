defmodule Kafkaesque.Core.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Kafkaesque.TopicRegistry},
      {DynamicSupervisor, name: Kafkaesque.TopicSupervisor, strategy: :one_for_one},
      {Phoenix.PubSub, name: Kafkaesque.PubSub},
      Kafkaesque.Telemetry.Supervisor
    ]

    opts = [strategy: :one_for_one, name: Kafkaesque.Core.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
