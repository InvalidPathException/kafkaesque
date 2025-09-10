defmodule KafkaesqueServer.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      KafkaesqueServer.Endpoint,
      {GRPC.Server.Supervisor,
       endpoint: KafkaesqueServer.GRPC.Endpoint, port: grpc_port(), start_server: true}
    ]

    opts = [strategy: :one_for_one, name: KafkaesqueServer.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp grpc_port do
    System.get_env("GRPC_PORT", "50051")
    |> String.to_integer()
  end
end
