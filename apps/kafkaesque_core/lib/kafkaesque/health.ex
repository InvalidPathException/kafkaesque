defmodule Kafkaesque.Health do
  @moduledoc """
  Health check module for Kafkaesque.
  Provides system health information and readiness checks.
  """

  alias Kafkaesque.Telemetry
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  @doc """
  Performs a comprehensive health check of the system.
  Returns {:ok, health_info} or {:error, reason}
  """
  def check do
    checks = [
      check_registry(),
      check_topics(),
      check_telemetry(),
      check_disk_space(),
      check_memory()
    ]

    failed_checks = Enum.filter(checks, fn {status, _, _} -> status == :error end)

    if Enum.empty?(failed_checks) do
      {:ok,
       %{
         status: :healthy,
         timestamp: System.system_time(:millisecond),
         checks:
           Enum.map(checks, fn {status, name, info} ->
             %{name: name, status: status, info: info}
           end)
       }}
    else
      {:error,
       %{
         status: :unhealthy,
         timestamp: System.system_time(:millisecond),
         checks:
           Enum.map(checks, fn {status, name, info} ->
             %{name: name, status: status, info: info}
           end),
         failed: Enum.map(failed_checks, fn {_, name, _} -> name end)
       }}
    end
  end

  @doc """
  Quick liveness check - returns :ok if the system is running.
  """
  def alive? do
    :ok
  end

  @doc """
  Readiness check - returns whether the system is ready to accept traffic.
  """
  def ready? do
    case check() do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp check_registry do
    case Process.whereis(Kafkaesque.TopicRegistry) do
      nil ->
        {:error, :registry, %{message: "Registry not running"}}

      pid when is_pid(pid) ->
        # Count registered processes
        entries =
          Registry.select(Kafkaesque.TopicRegistry, [{{:"$1", :"$2", :"$3"}, [], [:"$_"]}])

        {:ok, :registry, %{pid: pid, entries: length(entries)}}
    end
  end

  defp check_topics do
    topics = TopicSupervisor.list_topics()
    topic_count = length(topics)
    partition_count = Enum.reduce(topics, 0, fn topic, acc -> acc + topic.partitions end)

    {:ok, :topics,
     %{
       count: topic_count,
       partitions: partition_count,
       topics: Enum.map(topics, & &1.name)
     }}
  rescue
    _ -> {:error, :topics, %{message: "Failed to list topics"}}
  end

  defp check_telemetry do
    case Process.whereis(Kafkaesque.Telemetry) do
      nil ->
        {:error, :telemetry, %{message: "Telemetry not running"}}

      pid when is_pid(pid) ->
        try do
          metrics = Telemetry.get_metrics()

          {:ok, :telemetry,
           %{
             pid: pid,
             messages_per_sec: metrics[:messages_per_sec] || 0,
             bytes_per_sec: metrics[:bytes_per_sec] || 0
           }}
        rescue
          _ -> {:error, :telemetry, %{message: "Failed to get metrics"}}
        end
    end
  end

  defp check_disk_space do
    data_dir = Application.get_env(:kafkaesque_core, :data_dir, "./data")

    case File.stat(data_dir) do
      {:ok, _} ->
        # Try to get disk space info (OS-specific)
        case System.cmd("df", ["-k", data_dir], stderr_to_stdout: true) do
          {output, 0} ->
            # Parse df output (simplified)
            lines = String.split(output, "\n")

            if length(lines) >= 2 do
              parts =
                lines
                |> Enum.at(1)
                |> String.split()

              if length(parts) >= 4 do
                available = (parts |> Enum.at(3) |> String.to_integer()) * 1024

                used_percent =
                  parts |> Enum.at(4) |> String.trim_trailing("%") |> String.to_integer()

                status = if used_percent > 90, do: :warning, else: :ok

                {status, :disk,
                 %{
                   data_dir: data_dir,
                   available_bytes: available,
                   used_percent: used_percent
                 }}
              else
                {:ok, :disk, %{data_dir: data_dir, message: "Unable to parse disk info"}}
              end
            else
              {:ok, :disk, %{data_dir: data_dir, message: "Unable to get disk info"}}
            end

          _ ->
            {:ok, :disk, %{data_dir: data_dir, exists: true}}
        end

      _ ->
        {:error, :disk, %{message: "Data directory does not exist", path: data_dir}}
    end
  end

  defp check_memory do
    memory = :erlang.memory()
    total = memory[:total]
    processes = memory[:processes]
    ets = memory[:ets]

    # Warning if using more than 1GB
    status = if total > 1_073_741_824, do: :warning, else: :ok

    {status, :memory,
     %{
       total_bytes: total,
       processes_bytes: processes,
       ets_bytes: ets,
       total_mb: div(total, 1024 * 1024)
     }}
  end

  @doc """
  Returns basic system info for monitoring.
  """
  def info do
    %{
      version: "0.1.0",
      node: node(),
      uptime_ms: :erlang.statistics(:wall_clock) |> elem(0),
      schedulers: System.schedulers_online(),
      otp_release: :erlang.system_info(:otp_release) |> List.to_string(),
      elixir_version: System.version()
    }
  end
end
