defmodule KafkaesqueClient.Connection.Supervisor do
  @moduledoc """
  Supervises connection-related processes.
  """
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :unique, name: KafkaesqueClient.ConnectionRegistry},
      {DynamicSupervisor, name: KafkaesqueClient.ConnectionSupervisor, strategy: :one_for_one},
      {KafkaesqueClient.Connection.Pool, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
