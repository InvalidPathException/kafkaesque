defmodule Kafkaesque.Topic.RetentionController do
  @moduledoc """
  Manages logical retention for topics.
  Handles watermark advancement and marks data for cleanup based on retention policies.

  This is a simplified implementation that tracks retention windows but doesn't
  actually delete data (as we're using a single append-only file in the MVP).
  """

  use GenServer
  require Logger

  alias Kafkaesque.Storage.SingleFile

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
      find_offset_after_timestamp(state, cutoff_time)
    end
  end

  defp calculate_size_based_watermark(state) do
    if state.retention_bytes == nil do
      0
    else
      # Calculate total size and find offset where we exceed retention
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

  defp find_offset_after_timestamp(state, cutoff_time) do
    # Get offsets range first
    case SingleFile.get_offsets(state.topic, state.partition) do
      {:ok, %{earliest: earliest, latest: latest}} when latest > earliest ->
        # Try to get index table for more efficient search
        index_table = get_index_table(state)

        # Use index to narrow search if available
        {search_start, search_end} =
          if index_table do
            narrow_search_with_index(index_table, earliest, latest, cutoff_time)
          else
            {earliest, latest}
          end

        # Binary search to find the first offset after cutoff time
        binary_search_timestamp(state.topic, state.partition, search_start, search_end, cutoff_time)

      _ ->
        # No messages or error
        0
    end
  end

  defp binary_search_timestamp(topic, partition, low, high, cutoff_time) do
    # Binary search for the first message with timestamp >= cutoff_time
    binary_search_timestamp_impl(topic, partition, low, high, cutoff_time, nil)
  end

  defp binary_search_timestamp_impl(topic, partition, low, high, cutoff_time, best_found) do
    if low > high do
      # Search complete, return the best offset found
      best_found || 0
    else
      mid = div(low + high, 2)

      # Read a single message at the midpoint
      case SingleFile.read(topic, partition, mid, 1) do
        {:ok, [message]} ->
          message_time = message[:timestamp_ms] || 0

          if message_time >= cutoff_time do
            # This message is after cutoff, search earlier for a better match
            new_best = mid
            binary_search_timestamp_impl(topic, partition, low, mid - 1, cutoff_time, new_best)
          else
            # This message is before cutoff, search later
            binary_search_timestamp_impl(topic, partition, mid + 1, high, cutoff_time, best_found)
          end

        {:ok, []} ->
          # No message at this offset, try earlier
          binary_search_timestamp_impl(topic, partition, low, mid - 1, cutoff_time, best_found)

        _ ->
          # Error reading, try different range
          if mid > low do
            binary_search_timestamp_impl(topic, partition, low, mid - 1, cutoff_time, best_found)
          else
            best_found || 0
          end
      end
    end
  end

  defp find_offset_for_size_limit(state, size_limit_bytes) do
    # Calculate cumulative size and find offset where we exceed retention
    file_path = Path.join([state.storage_path, state.topic, "p-#{state.partition}.log"])

    case File.stat(file_path) do
      {:ok, %{size: file_size}} when file_size > size_limit_bytes ->
        # We need to keep only size_limit_bytes, so calculate how much to remove
        bytes_to_remove = file_size - size_limit_bytes

        # Try to use index for more efficient size calculation
        index_table = get_index_table(state)

        # Scan through messages to find where we've accumulated enough bytes to remove
        find_size_based_offset(state.topic, state.partition, bytes_to_remove, index_table)

      _ ->
        # File is smaller than limit, keep everything
        0
    end
  end

  defp find_size_based_offset(topic, partition, bytes_to_remove, index_table) do
    case SingleFile.get_offsets(topic, partition) do
      {:ok, %{earliest: earliest, latest: latest}} when latest > earliest ->
        # Use index entries to jump more efficiently if available
        if index_table do
          scan_with_index(topic, partition, index_table, bytes_to_remove, earliest, latest)
        else
          scan_for_size_limit_recursive(topic, partition, earliest, latest, bytes_to_remove, 0)
        end

      _ ->
        0
    end
  end

  defp scan_for_size_limit_recursive(
         topic,
         partition,
         current_offset,
         end_offset,
         bytes_to_remove,
         accumulated
       ) do
    if current_offset > end_offset or accumulated >= bytes_to_remove do
      # We've accumulated enough bytes to remove, return this offset as the new watermark
      current_offset
    else
      batch_size = min(50, end_offset - current_offset + 1)

      case SingleFile.read(topic, partition, current_offset, batch_size * 100) do
        {:ok, messages} when messages != [] ->
          # Calculate size of these messages
          batch_bytes = calculate_messages_size(messages)
          new_accumulated = accumulated + batch_bytes
          messages_count = length(messages)
          next_offset = current_offset + messages_count

          if new_accumulated >= bytes_to_remove do
            # Found the cutoff point - scan within this batch for exact offset
            find_exact_size_cutoff(messages, current_offset, bytes_to_remove, accumulated)
          else
            # Continue scanning
            scan_for_size_limit_recursive(
              topic,
              partition,
              next_offset,
              end_offset,
              bytes_to_remove,
              new_accumulated
            )
          end

        _ ->
          # No more messages or error, return current offset
          current_offset
      end
    end
  end

  defp find_exact_size_cutoff(messages, base_offset, bytes_to_remove, accumulated_before) do
    # Find the exact message where we cross the threshold
    {offset, _} =
      Enum.reduce_while(messages, {base_offset, accumulated_before}, fn msg, {offset, acc} ->
        msg_size = calculate_message_size(msg)
        new_acc = acc + msg_size

        if new_acc >= bytes_to_remove do
          {:halt, {offset + 1, new_acc}}
        else
          {:cont, {offset + 1, new_acc}}
        end
      end)

    offset
  end

  defp calculate_message_size(msg) do
    key_size = byte_size(msg[:key] || <<>>)
    value_size = byte_size(msg[:value] || <<>>)
    headers_size = calculate_headers_size(msg[:headers] || [])

    # Frame overhead: 4 (length) + 1 (magic) + 1 (attr) + 8 (timestamp) + 4 (key_len) + 4 (val_len) + 2 (headers_len)
    frame_overhead = 24
    key_size + value_size + headers_size + frame_overhead
  end

  defp calculate_messages_size(messages) do
    Enum.reduce(messages, 0, fn msg, acc ->
      key_size = byte_size(msg[:key] || <<>>)
      value_size = byte_size(msg[:value] || <<>>)
      headers_size = calculate_headers_size(msg[:headers] || [])

      # Frame overhead: 4 (length) + 1 (magic) + 1 (attr) + 8 (timestamp) + 4 (key_len) + 4 (val_len) + 2 (headers_len)
      frame_overhead = 24
      acc + key_size + value_size + headers_size + frame_overhead
    end)
  end

  defp calculate_headers_size(headers) do
    Enum.reduce(headers, 0, fn {k, v}, acc ->
      # Each header has: 2 (key_len) + key + 2 (val_len) + value
      acc + 4 + byte_size(to_string(k)) + byte_size(to_string(v))
    end)
  end

  defp calculate_retention_stats(state) do
    # Get statistics from storage
    stats =
      case Registry.lookup(
             Kafkaesque.TopicRegistry,
             {:storage, state.topic, state.partition}
           ) do
        [{pid, _}] when is_pid(pid) ->
          case SingleFile.get_offsets(state.topic, state.partition) do
            {:ok, %{earliest: earliest, latest: latest}} ->
              total_messages = latest - earliest
              effective_watermark = Map.values(state.watermarks) |> Enum.max()
              retained_messages = latest - effective_watermark

              # Get file size for byte statistics
              file_path = Path.join([state.storage_path, state.topic, "p-#{state.partition}.log"])

              {total_bytes, retained_bytes} =
                case File.stat(file_path) do
                  {:ok, %{size: size}} ->
                    retention_ratio =
                      if total_messages > 0 do
                        retained_messages / total_messages
                      else
                        1.0
                      end

                    {size, round(size * retention_ratio)}

                  _ ->
                    {0, 0}
                end

              %{
                total_messages: total_messages,
                retained_messages: retained_messages,
                total_bytes: total_bytes,
                retained_bytes: retained_bytes,
                earliest_offset: earliest,
                latest_offset: latest
              }

            _ ->
              %{
                total_messages: 0,
                retained_messages: 0,
                total_bytes: 0,
                retained_bytes: 0,
                earliest_offset: 0,
                latest_offset: 0
              }
          end

        _ ->
          %{
            total_messages: 0,
            retained_messages: 0,
            total_bytes: 0,
            retained_bytes: 0,
            earliest_offset: 0,
            latest_offset: 0
          }
      end

    Map.merge(stats, %{
      topic: state.topic,
      partition: state.partition,
      retention_ms: state.retention_ms,
      retention_bytes: state.retention_bytes,
      cleanup_policy: state.cleanup_policy,
      watermarks: state.watermarks,
      effective_watermark: Map.values(state.watermarks) |> Enum.max(),
      oldest_timestamp: System.system_time(:millisecond) - (state.retention_ms || 0),
      newest_timestamp: System.system_time(:millisecond)
    })
  end

  defp get_index_table(state) do
    # Get the index table from SingleFile
    case SingleFile.get_index_table(state.topic, state.partition) do
      {:ok, table} -> table
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp narrow_search_with_index(index_table, earliest, latest, _cutoff_time) do
    # Use index entries to narrow binary search range
    # Since we don't have timestamps in index yet, we can at least use
    # the index to skip large ranges
    entries = :ets.tab2list(index_table) |> Enum.sort()

    # For now, just use index to segment the search space
    # TODO: We could later enhance index to store timestamps
    if length(entries) > 10 do
      # Use middle third of the range for search (heuristic)
      range = latest - earliest
      search_start = earliest + div(range, 3)
      search_end = latest - div(range, 10)
      {search_start, search_end}
    else
      {earliest, latest}
    end
  end

  defp scan_with_index(topic, partition, index_table, bytes_to_remove, earliest, latest) do
    # Use index entries to jump through the file more efficiently
    entries = :ets.tab2list(index_table) |> Enum.sort()

    # Calculate approximate bytes per offset using index positions
    if length(entries) > 1 do
      # Use index to estimate where size limit is reached
      scan_with_index_entries(topic, partition, entries, bytes_to_remove, 0)
    else
      # Fall back to regular scan
      scan_for_size_limit_recursive(topic, partition, earliest, latest, bytes_to_remove, 0)
    end
  end

  defp scan_with_index_entries(topic, partition, entries, bytes_to_remove, accumulated) do
    case entries do
      [{offset, {position, _frame_len}} | rest] when rest != [] ->
        [{next_offset, {next_position, _}} | _] = rest

        # Approximate size between these index entries
        segment_size = next_position - position
        new_accumulated = accumulated + segment_size

        if new_accumulated >= bytes_to_remove do
          # Found the region, scan this segment in detail
          scan_for_exact_offset_in_segment(topic, partition, offset, next_offset,
                                          bytes_to_remove, accumulated)
        else
          scan_with_index_entries(topic, partition, rest, bytes_to_remove, new_accumulated)
        end

      _ ->
        # End of index or single entry
        0
    end
  end

  defp scan_for_exact_offset_in_segment(topic, partition, start_offset, end_offset,
                                       bytes_to_remove, accumulated_before) do
    # Scan the specific segment to find exact offset
    batch_size = min(50, end_offset - start_offset + 1)

    case SingleFile.read(topic, partition, start_offset, batch_size * 100) do
      {:ok, messages} when messages != [] ->
        # Find exact cutoff in this batch
        find_exact_size_cutoff(messages, start_offset, bytes_to_remove, accumulated_before)

      _ ->
        start_offset
    end
  end
end
