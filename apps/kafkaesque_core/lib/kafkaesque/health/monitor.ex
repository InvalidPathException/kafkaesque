defmodule Kafkaesque.Health.Monitor do
  @moduledoc """
  Continuous health monitoring for Kafkaesque with automatic status updates.
  Performs periodic health checks and maintains system health state.

  ## Features
  - Automatic health checks every 5 seconds
  - Failure thresholds before marking unhealthy
  - Custom health check registration
  - Integration with monitoring systems
  """

  use GenServer
  require Logger

  @check_interval 5_000
  @unhealthy_threshold 3

  defstruct [
    :checks,
    :status,
    :failures,
    :last_check_time,
    :check_results
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Get current health status.
  """
  def status do
    GenServer.call(__MODULE__, :status)
  catch
    :exit, {:noproc, _} -> {:error, :monitor_not_running}
  end

  @doc """
  Register a custom health check function.
  """
  def register_check(name, fun) when is_atom(name) and is_function(fun, 0) do
    GenServer.call(__MODULE__, {:register, name, fun})
  end

  @doc """
  Unregister a health check.
  """
  def unregister_check(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:unregister, name})
  end

  @doc """
  Get detailed health information.
  """
  def get_health_info do
    GenServer.call(__MODULE__, :get_health_info)
  catch
    :exit, {:noproc, _} -> {:error, :monitor_not_running}
  end

  @doc """
  Force an immediate health check.
  """
  def check_now do
    GenServer.cast(__MODULE__, :check_now)
  end

  @impl true
  def init(_opts) do
    # Schedule first check
    schedule_check()

    state = %__MODULE__{
      checks: %{},
      status: :healthy,
      failures: %{},
      last_check_time: nil,
      check_results: %{}
    }

    # Register default checks
    default_checks = %{
      storage: &check_storage/0,
      memory: &check_memory/0,
      process_count: &check_process_count/0,
      disk_space: &check_disk_space/0,
      telemetry: &check_telemetry/0
    }

    Logger.info("Health monitor started with #{map_size(default_checks)} default checks")
    {:ok, %{state | checks: default_checks}}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state.status, state}
  end

  def handle_call(:get_health_info, _from, state) do
    info = %{
      status: state.status,
      last_check: state.last_check_time,
      checks: state.check_results,
      failures: state.failures
    }
    {:reply, info, state}
  end

  def handle_call({:register, name, fun}, _from, state) do
    Logger.info("Registered health check: #{name}")
    new_checks = Map.put(state.checks, name, fun)
    {:reply, :ok, %{state | checks: new_checks}}
  end

  def handle_call({:unregister, name}, _from, state) do
    Logger.info("Unregistered health check: #{name}")
    new_checks = Map.delete(state.checks, name)
    new_failures = Map.delete(state.failures, name)
    new_results = Map.delete(state.check_results, name)

    {:reply, :ok,
     %{state |
       checks: new_checks,
       failures: new_failures,
       check_results: new_results}}
  end

  @impl true
  def handle_cast(:check_now, state) do
    {:noreply, perform_health_checks(state)}
  end

  @impl true
  def handle_info(:check, state) do
    new_state = perform_health_checks(state)
    schedule_check()
    {:noreply, new_state}
  end

  defp perform_health_checks(state) do
    results = Enum.map(state.checks, fn {name, check_fun} ->
      {name, safe_check(check_fun)}
    end)

    # Update failure counts
    failures = Enum.reduce(results, state.failures, fn {name, result}, acc ->
      case result do
        {:ok, _} -> Map.delete(acc, name)
        {:warning, _} -> Map.put(acc, name, Map.get(acc, name, 0))  # Don't increment for warnings
        {:error, _} -> Map.update(acc, name, 1, &(&1 + 1))
      end
    end)

    # Determine overall status
    status = cond do
      # Any critical errors with threshold exceeded
      Enum.any?(failures, fn {_, count} -> count >= @unhealthy_threshold end) ->
        :unhealthy

      # Any warnings or non-critical errors
      Enum.any?(results, fn {_, r} -> match?({:warning, _}, r) end) ->
        :degraded

      # All checks passed
      true ->
        :healthy
    end

    # Log status changes
    if status != state.status do
      case status do
        :unhealthy ->
          Logger.error("System health changed to UNHEALTHY")
        :degraded ->
          Logger.warning("System health changed to DEGRADED")
        :healthy ->
          Logger.info("System health changed to HEALTHY")
      end

      # Broadcast health status change
      Phoenix.PubSub.broadcast(
        Kafkaesque.PubSub,
        "health:status",
        {:health_status_changed, status}
      )
    end

    # Convert results to map for easier access
    check_results = Enum.into(results, %{})

    %{state |
      failures: failures,
      status: status,
      last_check_time: System.system_time(:millisecond),
      check_results: check_results}
  end

  defp safe_check(fun) do
    fun.()
  rescue
    error -> {:error, Exception.message(error)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp check_storage do
    # Check if storage processes are running
    case Registry.select(Kafkaesque.TopicRegistry, [
      {{{:storage, :_, :_}, :"$1", :_}, [], [:"$1"]}
    ]) do
      [] ->
        {:warning, "No storage processes running"}

      pids ->
        alive_count = Enum.count(pids, &Process.alive?/1)
        total_count = length(pids)

        if alive_count == total_count do
          {:ok, %{storage_processes: alive_count}}
        else
          {:error, "#{total_count - alive_count} storage processes down"}
        end
    end
  end

  defp check_memory do
    memory = :erlang.memory()
    total = memory[:total]
    processes = memory[:processes]
    ets = memory[:ets]

    total_mb = div(total, 1024 * 1024)

    cond do
      total > 2_000_000_000 ->  # 2GB
        {:error, %{total_mb: total_mb, message: "Memory usage critical"}}

      total > 1_000_000_000 ->  # 1GB
        {:warning, %{total_mb: total_mb, message: "Memory usage high"}}

      true ->
        {:ok, %{
          total_mb: total_mb,
          processes_mb: div(processes, 1024 * 1024),
          ets_mb: div(ets, 1024 * 1024)
        }}
    end
  end

  defp check_process_count do
    count = :erlang.system_info(:process_count)
    limit = :erlang.system_info(:process_limit)
    usage_percent = round(count * 100 / limit)

    cond do
      count > 100_000 ->
        {:error, %{count: count, limit: limit, usage_percent: usage_percent}}

      usage_percent > 80 ->
        {:warning, %{count: count, limit: limit, usage_percent: usage_percent}}

      true ->
        {:ok, %{count: count, limit: limit, usage_percent: usage_percent}}
    end
  end

  defp check_disk_space do
    data_dir = Application.get_env(:kafkaesque_core, :data_dir, "./data")

    case File.stat(data_dir) do
      {:ok, _} ->
        case System.cmd("df", ["-k", data_dir], stderr_to_stdout: true) do
          {output, 0} ->
            lines = String.split(output, "\n")

            if length(lines) >= 2 do
              parts = lines |> Enum.at(1) |> String.split()

              if length(parts) >= 4 do
                available = (parts |> Enum.at(3) |> String.to_integer()) * 1024
                used_percent = parts |> Enum.at(4) |> String.trim_trailing("%") |> String.to_integer()

                cond do
                  used_percent > 95 ->
                    {:error, %{used_percent: used_percent, available_mb: div(available, 1024 * 1024)}}

                  used_percent > 90 ->
                    {:warning, %{used_percent: used_percent, available_mb: div(available, 1024 * 1024)}}

                  true ->
                    {:ok, %{used_percent: used_percent, available_mb: div(available, 1024 * 1024)}}
                end
              else
                {:ok, %{data_dir: data_dir}}
              end
            else
              {:ok, %{data_dir: data_dir}}
            end

          _ ->
            {:ok, %{data_dir: data_dir, note: "Unable to get disk stats"}}
        end

      _ ->
        {:error, "Data directory does not exist: #{data_dir}"}
    end
  end

  defp check_telemetry do
    case Process.whereis(Kafkaesque.Telemetry) do
      nil ->
        {:error, "Telemetry process not running"}

      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          try do
            metrics = Kafkaesque.Telemetry.get_metrics()
            {:ok, %{
              messages_per_sec: metrics[:messages_per_sec] || 0,
              bytes_per_sec: metrics[:bytes_per_sec] || 0
            }}
          rescue
            _ -> {:error, "Failed to get telemetry metrics"}
          end
        else
          {:error, "Telemetry process not alive"}
        end
    end
  end

  defp schedule_check do
    Process.send_after(self(), :check, @check_interval)
  end
end
