defmodule Kafkaesque.Telemetry.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      # Telemetry poller will execute the given period measurements
      {:telemetry_poller,
       measurements: periodic_measurements(),
       period: :timer.seconds(10),
       name: Kafkaesque.Telemetry.Poller}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp periodic_measurements do
    [
      # Add periodic measurements here
    ]
  end
end
