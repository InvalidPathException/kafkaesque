defmodule KafkaesqueDashboard.Telemetry do
  use Supervisor
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      {:telemetry_poller, measurements: periodic_measurements(), period: 10_000}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def metrics do
    [
      # Phoenix Metrics
      summary("phoenix.endpoint.stop.duration",
        unit: {:native, :millisecond}
      ),
      summary("phoenix.router_dispatch.stop.duration",
        tags: [:route],
        unit: {:native, :millisecond}
      ),

      # Kafkaesque Metrics
      counter("kafkaesque.produce.count"),
      summary("kafkaesque.produce.latency"),
      counter("kafkaesque.consume.count"),
      summary("kafkaesque.consume.latency"),
      last_value("kafkaesque.topic.offset.latest"),
      last_value("kafkaesque.topic.offset.earliest"),
      last_value("kafkaesque.consumer_group.lag"),

      # VM Metrics
      summary("vm.memory.total", unit: {:byte, :megabyte}),
      summary("vm.total_run_queue_lengths.total"),
      summary("vm.total_run_queue_lengths.cpu"),
      summary("vm.total_run_queue_lengths.io")
    ]
  end

  defp periodic_measurements do
    [
      # Add VM measurements
    ]
  end
end
