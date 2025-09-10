defmodule KafkaesqueDashboard.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      KafkaesqueDashboard.Telemetry,
      {Phoenix.PubSub, name: KafkaesqueDashboard.PubSub},
      KafkaesqueDashboard.Endpoint
    ]

    opts = [strategy: :one_for_one, name: KafkaesqueDashboard.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    KafkaesqueDashboard.Endpoint.config_change(changed, removed)
    :ok
  end
end
