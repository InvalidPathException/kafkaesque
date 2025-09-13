defmodule Kafkaesque.Topic.RetentionController do
  @moduledoc """
  Manages logical retention for topics.
  Handles watermark advancement and marks data for cleanup based on retention policies.

  This is a simplified implementation that tracks retention windows but doesn't
  actually delete data (as we're using a single append-only file in the MVP).
  """

  use GenServer
  require Logger

  # Check retention every minute
  @check_interval_ms 60_000
  # 7 days
  @default_retention_ms 7 * 24 * 60 * 60 * 1000

  defmodule State do
    @moduledoc false
    defstruct [
      :topic,
      :partition,
      :retention_ms,
      :retention_bytes,
      :cleanup_policy,
      :watermarks,
      :index_table,
      :storage_path
    ]
  end

  @doc """
  Starts the retention controller for a topic-partition.
  """
  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    name = via_tuple(topic, partition)

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets the current retention watermark for a topic-partition.
  Returns the oldest offset that is still retained.
  """
  def get_watermark(topic, partition) do
    GenServer.call(via_tuple(topic, partition), :get_watermark)
  end

  @doc """
  Updates retention configuration for a topic-partition.
  """
  def update_config(topic, partition, config) do
    GenServer.cast(via_tuple(topic, partition), {:update_config, config})
  end

  @doc """
  Forces an immediate retention check.
  """
  def check_retention_now(topic, partition) do
    GenServer.cast(via_tuple(topic, partition), :check_retention)
  end

  @doc """
  Gets retention statistics for monitoring.
  """
  def get_stats(topic, partition) do
    GenServer.call(via_tuple(topic, partition), :get_stats)
  end

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    index_table = Keyword.get(opts, :index_table)
    storage_path = Keyword.get(opts, :storage_path, "/var/lib/kafkaesque/data")

    # Get retention configuration
    retention_ms = Keyword.get(opts, :retention_ms, @default_retention_ms)
    retention_bytes = Keyword.get(opts, :retention_bytes, nil)
    cleanup_policy = Keyword.get(opts, :cleanup_policy, :delete)

    state = %State{
      topic: topic,
      partition: partition,
      retention_ms: retention_ms,
      retention_bytes: retention_bytes,
      cleanup_policy: cleanup_policy,
      watermarks: %{
        time_based: 0,
        size_based: 0,
        compaction: 0
      },
      index_table: index_table,
      storage_path: storage_path
    }

    # Schedule first retention check
    schedule_retention_check()

    Logger.info(
      "RetentionController started for #{topic}-#{partition} with retention #{retention_ms}ms"
    )

    {:ok, state}
  end

  @impl true
  def handle_call(:get_watermark, _from, state) do
    # Return the highest watermark (most restrictive)
    watermark =
      state.watermarks
      |> Map.values()
      |> Enum.max()

    {:reply, {:ok, watermark}, state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats = calculate_retention_stats(state)
    {:reply, {:ok, stats}, state}
  end

  @impl true
  def handle_cast({:update_config, config}, state) do
    new_state = %{
      state
      | retention_ms: Map.get(config, :retention_ms, state.retention_ms),
        retention_bytes: Map.get(config, :retention_bytes, state.retention_bytes),
        cleanup_policy: Map.get(config, :cleanup_policy, state.cleanup_policy)
    }

    Logger.info("Updated retention config for #{state.topic}-#{state.partition}")

    {:noreply, new_state}
  end

  @impl true
  def handle_cast(:check_retention, state) do
    new_state = perform_retention_check(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:scheduled_retention_check, state) do
    new_state = perform_retention_check(state)
    schedule_retention_check()
    {:noreply, new_state}
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:retention_controller, topic, partition}}}
  end

  defp schedule_retention_check do
    Process.send_after(self(), :scheduled_retention_check, @check_interval_ms)
  end

  defp perform_retention_check(state) do
    Logger.debug("Performing retention check for #{state.topic}-#{state.partition}")

    # Calculate new watermarks based on different policies
    new_watermarks = %{
      time_based: calculate_time_based_watermark(state),
      size_based: calculate_size_based_watermark(state),
      compaction: calculate_compaction_watermark(state)
    }

    # Log watermark advancement
    if new_watermarks != state.watermarks do
      Logger.info(
        "Advanced watermarks for #{state.topic}-#{state.partition}: " <>
          "time=#{new_watermarks.time_based}, size=#{new_watermarks.size_based}"
      )

      # Emit telemetry event
      :telemetry.execute(
        [:kafkaesque, :retention, :watermark_advanced],
        %{
          old_watermark: Map.values(state.watermarks) |> Enum.max(),
          new_watermark: Map.values(new_watermarks) |> Enum.max()
        },
        %{
          topic: state.topic,
          partition: state.partition
        }
      )
    end

    %{state | watermarks: new_watermarks}
  end

  defp calculate_time_based_watermark(state) do
    if state.retention_ms == nil do
      0
    else
      current_time = System.system_time(:millisecond)
      cutoff_time = current_time - state.retention_ms

      # Find the offset of the first message after the cutoff time
      # In a real implementation, this would scan the index
      find_offset_after_timestamp(state, cutoff_time)
    end
  end

  defp calculate_size_based_watermark(state) do
    if state.retention_bytes == nil do
      0
    else
      # Calculate total size and find offset where we exceed retention
      # In a real implementation, this would use the index and file stats
      find_offset_for_size_limit(state, state.retention_bytes)
    end
  end

  defp calculate_compaction_watermark(state) do
    if state.cleanup_policy != :compact do
      0
    else
      # For compaction, we'd track the offset up to which compaction has been performed
      # This is a placeholder for the MVP
      0
    end
  end

  defp find_offset_after_timestamp(state, _cutoff_time) do
    # Mock implementation - in reality would scan the index
    # Returns an offset that would retain messages newer than cutoff_time

    if state.index_table do
      # Scan ETS table for entries with timestamp > cutoff_time
      # This is a simplified mock
      :rand.uniform(1000)
    else
      0
    end
  end

  defp find_offset_for_size_limit(state, size_limit_bytes) do
    # Mock implementation - in reality would calculate cumulative size
    # Returns an offset that keeps total size under the limit

    file_path = Path.join([state.storage_path, state.topic, "#{state.partition}.log"])

    case File.stat(file_path) do
      {:ok, %{size: file_size}} when file_size > size_limit_bytes ->
        # Calculate approximate offset based on file size
        # This is very simplified - real implementation would be precise
        retention_ratio = size_limit_bytes / file_size
        estimated_offset = round(1000 * (1 - retention_ratio))
        max(0, estimated_offset)

      _ ->
        0
    end
  end

  defp calculate_retention_stats(state) do
    %{
      topic: state.topic,
      partition: state.partition,
      retention_ms: state.retention_ms,
      retention_bytes: state.retention_bytes,
      cleanup_policy: state.cleanup_policy,
      watermarks: state.watermarks,
      effective_watermark: Map.values(state.watermarks) |> Enum.max(),
      # Mock statistics - in reality would calculate from actual data
      total_messages: :rand.uniform(100_000),
      retained_messages: :rand.uniform(50_000),
      total_bytes: :rand.uniform(10_000_000),
      retained_bytes: :rand.uniform(5_000_000),
      oldest_timestamp: System.system_time(:millisecond) - state.retention_ms,
      newest_timestamp: System.system_time(:millisecond)
    }
  end
end
