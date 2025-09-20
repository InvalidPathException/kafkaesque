defmodule KafkaesqueDashboard.Application do
  @moduledoc false

  use Application

  alias KafkaesqueDashboard.{Endpoint, Telemetry}
  alias LiveSvelte.SSR

  @impl true
  def start(_type, _args) do
    children = [
      {NodeJS.Supervisor, [path: SSR.NodeJS.server_path(), pool_size: 4]},
      Telemetry,
      KafkaesqueDashboard.MetricsCache,
      Endpoint
    ]

    opts = [strategy: :one_for_one, name: KafkaesqueDashboard.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    Endpoint.config_change(changed, removed)
    :ok
  end
end
