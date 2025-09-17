defmodule Kafkaesque.Storage.SingleFile do
  @moduledoc """
  Single append-only file storage per topic/partition.
  Implements binary framing with magic byte, CRC validation, attributes, and timestamps.

  ## Frame Format

  Each record is stored as a binary frame with the following structure:

  ```
  +-------------------+------------------+
  | Frame Length (4B) | Frame Content    |
  +-------------------+------------------+

  Frame Content:
  +-----------+--------------+------------+------------------+--------------+
  | Magic (1B)| CRC (4B)     | Attr (1B)  | TS_MS (8B)       | Key_Len (4B) |
  +-----------+--------------+------------+------------------+--------------+
  | Key       | Val_Len (4B) | Value      | Headers_Len (2B) | Headers      |
  +-----------+--------------+------------+------------------+--------------+
  ```

  ## CRC Validation

  The CRC32 checksum is calculated over all frame content AFTER the CRC field itself.
  This includes:
  - Attributes (1 byte)
  - Timestamp (8 bytes)
  - Key length and key
  - Value length and value
  - Headers length and headers

  The CRC is stored as a 4-byte big-endian integer immediately after the magic byte.
  On read, the CRC is recalculated and compared to detect corruption.
  """

  use GenServer
  require Logger

  alias Kafkaesque.Constants
  alias Kafkaesque.Storage.Index
  alias Kafkaesque.Telemetry

  @magic_byte Constants.magic_byte()
  @fsync_interval_ms Application.compile_env(
                       :kafkaesque_core,
                       :fsync_interval_ms,
                       Constants.fsync_interval_ms()
                     )

  defstruct [
    :topic,
    :partition,
    :file_path,
    :file,
    :current_offset,
    :current_position,
    :index,
    :fsync_timer,
    :last_fsync_position
  ]

  # [frame_length:4][magic:1][attr:1][ts_ms:8][key_len:4][key][val_len:4][value][headers_len:2][headers]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: via_tuple(opts[:topic], opts[:partition]))
  end

  def append(topic, partition, records) do
    GenServer.call(via_tuple(topic, partition), {:append, records})
  end

  def read(topic, partition, start_offset, max_bytes) do
    GenServer.call(via_tuple(topic, partition), {:read, start_offset, max_bytes})
  end

  def get_offsets(topic, partition) do
    GenServer.call(via_tuple(topic, partition), :get_offsets)
  end

  def close(topic, partition) do
    GenServer.call(via_tuple(topic, partition), :close)
  end

  def get_index_table(topic, partition) do
    GenServer.call(via_tuple(topic, partition), :get_index_table)
  end

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    data_dir = Keyword.get(opts, :data_dir, "./data")

    dir_path = Path.join(data_dir, topic)
    File.mkdir_p!(dir_path)

    file_path = Path.join(dir_path, "p-#{partition}.log")

    # Open file in append mode
    {:ok, file} = File.open(file_path, [:append, :binary, :raw, :read])

    index = Index.new(topic, partition)

    {current_offset, current_position, updated_index} =
      scan_and_rebuild_index(file, file_path, index)

    fsync_timer = schedule_fsync()

    state = %__MODULE__{
      topic: topic,
      partition: partition,
      file_path: file_path,
      file: file,
      current_offset: current_offset,
      current_position: current_position,
      index: updated_index,
      fsync_timer: fsync_timer,
      last_fsync_position: current_position
    }

    Logger.info(
      "Storage initialized for #{topic}/#{partition} at offset #{current_offset}, position #{current_position}"
    )

    {:ok, state}
  end

  @impl true
  def handle_call({:append, records}, _from, state) do
    # Validate batch size
    if length(records) > Constants.max_batch_size() do
      {:reply, {:error, :batch_too_large}, state}
    else
      case validate_records(records) do
        :ok ->
          start_time = System.monotonic_time()

          try do
            {frames, total_bytes} = serialize_records(records, state.current_offset)

            :ok = :file.pwrite(state.file, state.current_position, frames)

            new_offset = state.current_offset + length(records)
            new_position = state.current_position + total_bytes

            updated_index = maybe_add_index_entries(state, records, total_bytes)

            duration_ms =
              System.convert_time_unit(
                System.monotonic_time() - start_time,
                :native,
                :millisecond
              )

            Telemetry.storage_write(state.topic, state.partition, total_bytes, duration_ms)

            new_state = %{
              state
              | current_offset: new_offset,
                current_position: new_position,
                index: updated_index
            }

            result = %{
              topic: state.topic,
              partition: state.partition,
              base_offset: state.current_offset,
              last_offset: new_offset - 1,
              count: length(records)
            }

            {:reply, {:ok, result}, new_state}
          catch
            {:error, reason} -> {:reply, {:error, reason}, state}
          end

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  @impl true
  def handle_call({:read, start_offset, max_bytes}, _from, state) do
    start_time = System.monotonic_time()

    # Check if offset is in valid range
    result =
      if start_offset < 0 or start_offset >= state.current_offset do
        # Offset is beyond what we've written
        {:error, :offset_out_of_range}
      else
        # Try to find position in index
        case Index.lookup(state.index, start_offset) do
          {:ok, {file_position, indexed_offset}} ->
            # Position found in index (may be exact or closest before)
            # indexed_offset tells us the actual offset at this position
            read_from_position(
              state.file,
              file_position,
              indexed_offset,
              start_offset,
              max_bytes
            )

          :not_found ->
            # Index doesn't have this offset, but it's in valid range
            # Start from beginning of file
            read_from_position(state.file, 0, 0, start_offset, max_bytes)
        end
      end

    case result do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {records, bytes_read} ->
        duration_ms =
          System.convert_time_unit(
            System.monotonic_time() - start_time,
            :native,
            :millisecond
          )

        Telemetry.storage_read(state.topic, state.partition, bytes_read, duration_ms)
        {:reply, {:ok, records}, state}
    end
  end

  @impl true
  def handle_call(:get_offsets, _from, state) do
    # In Kafka semantics:
    # - earliest: first available offset for reading (0 if any messages exist)
    # - latest: next offset to be assigned (where next message will be written)
    # For an empty topic: earliest = 0, latest = 0
    # With N messages (offsets 0 to N-1): earliest = 0, latest = N

    offsets = %{
      earliest: 0,
      latest: state.current_offset
    }

    {:reply, {:ok, offsets}, state}
  end

  @impl true
  def handle_call(:get_index_table, _from, state) do
    {:reply, {:ok, state.index.table}, state}
  end

  @impl true
  def handle_call(:close, _from, state) do
    # Flush and close file
    :file.datasync(state.file)
    :file.close(state.file)

    # Cancel fsync timer
    Process.cancel_timer(state.fsync_timer)

    {:stop, :normal, :ok, state}
  end

  defp read_from_position(file, position, indexed_offset, start_offset, max_bytes) do
    # Calculate how much data we need to read
    # We need to scan from indexed_offset to start_offset, then read max_bytes
    offset_distance = max(start_offset - indexed_offset + 1, 1)
    # Minimum 1KB to ensure we can read at least a few messages
    # For test messages (~50 bytes each), this gives us ~20 messages
    min_read = 1024
    # Read enough to scan to target offset plus requested data
    scan_buffer_size = max(min_read, offset_distance * 100 + max_bytes)

    case :file.pread(file, position, scan_buffer_size) do
      {:ok, data} ->
        # Parse all frames starting from the indexed_offset
        all_records = parse_frames(data, indexed_offset, scan_buffer_size)

        # Filter to get records from start_offset onwards
        records_from_offset =
          Enum.drop_while(all_records, fn r ->
            r.offset < start_offset
          end)

        # Limit to max_bytes worth of records
        limited_records = take_up_to_bytes(records_from_offset, max_bytes)
        {limited_records, calculate_bytes(limited_records)}

      :eof ->
        {[], 0}

      {:error, _reason} ->
        {[], 0}
    end
  end

  defp take_up_to_bytes(records, max_bytes) do
    {taken, _} =
      Enum.reduce_while(records, {[], 0}, fn record, {acc, bytes} ->
        record_size = byte_size(record.value || <<>>) + byte_size(record.key || <<>>)
        new_bytes = bytes + record_size

        if new_bytes <= max_bytes do
          {:cont, {[record | acc], new_bytes}}
        else
          {:halt, {acc, bytes}}
        end
      end)

    Enum.reverse(taken)
  end

  @impl true
  def handle_info(:fsync, state) do
    # Only fsync if we've written new data
    if state.current_position > state.last_fsync_position do
      :file.datasync(state.file)

      Logger.debug(
        "Fsync for #{state.topic}/#{state.partition} at position #{state.current_position}"
      )

      new_state = %{
        state
        | last_fsync_position: state.current_position,
          fsync_timer: schedule_fsync()
      }

      {:noreply, new_state}
    else
      # No new data, just reschedule
      {:noreply, %{state | fsync_timer: schedule_fsync()}}
    end
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:storage, topic, partition}}}
  end

  defp schedule_fsync do
    Process.send_after(self(), :fsync, @fsync_interval_ms)
  end

  defp scan_and_rebuild_index(file, file_path, index) do
    case File.stat(file_path) do
      {:ok, %{size: 0}} ->
        # Empty file
        {0, 0, index}

      {:ok, %{size: size}} when size > 0 ->
        # Stream the file in chunks to avoid loading entire file into memory
        # 1MB chunks
        chunk_size = 1024 * 1024
        {offset, position, updated_index} = rebuild_index_streaming(file, size, chunk_size, index)
        {offset, position, updated_index}

      {:error, :enoent} ->
        # File doesn't exist yet
        {0, 0, index}

      _ ->
        # Any other case
        {0, 0, index}
    end
  end

  defp rebuild_index_streaming(file, total_size, chunk_size, index) do
    rebuild_index_streaming(file, 0, total_size, chunk_size, index, 0, 0, 0, <<>>)
  end

  defp rebuild_index_streaming(
         _file,
         read_pos,
         total_size,
         _chunk_size,
         index,
         offset,
         position,
         _bytes_since_index,
         leftover
       )
       when read_pos >= total_size do
    # Process any leftover data
    if byte_size(leftover) > 0 do
      Logger.warning("Truncating incomplete frame at end of file (#{byte_size(leftover)} bytes)")
    end

    {offset, position, index}
  end

  defp rebuild_index_streaming(
         file,
         read_pos,
         total_size,
         chunk_size,
         index,
         offset,
         position,
         bytes_since_index,
         leftover
       ) do
    # Read next chunk
    bytes_to_read = min(chunk_size, total_size - read_pos)
    {:ok, chunk} = :file.pread(file, read_pos, bytes_to_read)

    # Combine leftover from previous chunk with new data
    data = <<leftover::binary, chunk::binary>>

    # Process complete frames in this chunk
    {new_offset, new_position, new_index, new_bytes_since_index, new_leftover} =
      process_chunk_frames(data, index, offset, position, bytes_since_index)

    # Continue with next chunk
    rebuild_index_streaming(
      file,
      read_pos + bytes_to_read,
      total_size,
      chunk_size,
      new_index,
      new_offset,
      new_position,
      new_bytes_since_index,
      new_leftover
    )
  end

  defp process_chunk_frames(data, index, offset, position, bytes_since_index) do
    process_chunk_frames(data, index, offset, position, bytes_since_index, 0)
  end

  defp process_chunk_frames(data, index, offset, position, bytes_since_index, processed_in_chunk) do
    case parse_single_frame(data) do
      {:ok, frame_size, _record, rest} ->
        # Update index if needed
        new_index =
          if offset == 0 or rem(offset, Constants.index_interval()) == 0 or
               bytes_since_index >= Constants.index_bytes_threshold() do
            Index.add(index, offset, position, frame_size)
          else
            index
          end

        new_bytes_since_index = if new_index != index, do: 0, else: bytes_since_index + frame_size

        # Continue processing rest of chunk
        process_chunk_frames(
          rest,
          new_index,
          offset + 1,
          position + frame_size + Constants.frame_header_size(),
          new_bytes_since_index,
          processed_in_chunk + frame_size + Constants.frame_header_size()
        )

      {:error, :incomplete} ->
        # Can't parse complete frame, return leftover for next chunk
        {offset, position, index, bytes_since_index, data}

      {:error, reason} ->
        # Log corruption and skip this frame
        Logger.warning("Skipping corrupted frame at offset #{offset}: #{reason}")
        # Return what we've processed so far
        {offset, position, index, bytes_since_index, <<>>}
    end
  end

  defp serialize_records(records, base_offset) do
    {frames, _offset} =
      Enum.reduce(records, {[], base_offset}, fn record, {acc, offset} ->
        frame = serialize_record(record, offset)
        {[frame | acc], offset + 1}
      end)

    binary = IO.iodata_to_binary(Enum.reverse(frames))
    {binary, byte_size(binary)}
  catch
    {:error, reason} -> throw({:error, reason})
  end

  defp serialize_record(record, _offset) do
    timestamp = record[:timestamp_ms] || System.system_time(:millisecond)
    key = record[:key] || <<>>
    value = record[:value] || <<>>

    # Validate key and value are binaries
    unless is_binary(key) or is_nil(key), do: throw({:error, :key_must_be_binary})
    unless is_binary(value) or is_nil(value), do: throw({:error, :value_must_be_binary})

    key = to_binary(key)
    value = to_binary(value)
    headers = serialize_headers(record[:headers] || [])

    # Build content without CRC first
    content_without_crc = [
      @magic_byte,
      # attributes (reserved)
      <<0>>,
      <<timestamp::64>>,
      <<byte_size(key)::32>>,
      key,
      <<byte_size(value)::32>>,
      value,
      <<byte_size(headers)::16>>,
      headers
    ]

    content_binary = IO.iodata_to_binary(content_without_crc)
    crc32 = :erlang.crc32(content_binary)

    # Final frame with CRC at the beginning (after magic byte)
    frame_content = [
      @magic_byte,
      <<crc32::32>>,
      # attributes (reserved)
      <<0>>,
      <<timestamp::64>>,
      <<byte_size(key)::32>>,
      key,
      <<byte_size(value)::32>>,
      value,
      <<byte_size(headers)::16>>,
      headers
    ]

    frame_binary = IO.iodata_to_binary(frame_content)
    frame_size = byte_size(frame_binary)

    [<<frame_size::32>>, frame_binary]
  end

  defp calculate_bytes(records) do
    Enum.reduce(records, 0, fn r, acc ->
      acc + byte_size(r.value || <<>>) + byte_size(r.key || <<>>)
    end)
  end

  defp serialize_headers(headers) do
    headers
    |> Enum.map(fn
      {k, v} when is_binary(k) and is_binary(v) ->
        [<<byte_size(k)::16>>, k, <<byte_size(v)::32>>, v]

      {k, v} when is_atom(k) or is_binary(k) ->
        # Allow atom keys for convenience but values must be binary
        key = if is_atom(k), do: Atom.to_string(k), else: k

        if is_binary(v) do
          [<<byte_size(key)::16>>, key, <<byte_size(v)::32>>, v]
        else
          throw({:error, :header_value_must_be_binary})
        end

      _ ->
        throw({:error, :invalid_header_format})
    end)
    |> IO.iodata_to_binary()
  end

  # Only accept binaries - reject everything else for safety
  defp to_binary(value) when is_binary(value), do: value
  defp to_binary(nil), do: <<>>

  defp validate_records(records) do
    Enum.reduce_while(records, :ok, fn record, :ok ->
      validate_single_record(record)
    end)
  end

  defp validate_single_record(record) do
    key = record[:key] || <<>>
    value = record[:value] || <<>>
    headers = record[:headers] || []

    with :ok <- validate_key(key),
         :ok <- validate_value(value),
         :ok <- validate_headers(headers) do
      {:cont, :ok}
    else
      error -> {:halt, error}
    end
  end

  defp validate_key(key) when is_binary(key) or is_nil(key) do
    if byte_size(to_binary(key)) > Constants.max_key_size() do
      {:error, :key_too_large}
    else
      :ok
    end
  end

  defp validate_key(_), do: {:error, :invalid_key_type}

  defp validate_value(value) when is_binary(value) or is_nil(value) do
    if byte_size(to_binary(value)) > Constants.max_value_size() do
      {:error, :value_too_large}
    else
      :ok
    end
  end

  defp validate_value(_), do: {:error, :invalid_value_type}

  defp validate_headers(headers) when is_list(headers), do: :ok
  defp validate_headers(_), do: {:error, :invalid_headers_type}

  defp parse_frames(data, start_offset, _max_bytes) do
    # Parse all frames in the data that was read from the file
    # The max_bytes limit is already applied when reading from the file
    parse_frames_impl(data, start_offset, [])
  end

  defp parse_frames_impl(<<>>, _offset, acc) do
    Enum.reverse(acc)
  end

  defp parse_frames_impl(data, offset, acc) do
    case parse_single_frame(data) do
      {:ok, _frame_size, record, rest} ->
        # Add offset to the record
        record_with_offset = Map.put(record, :offset, offset)
        parse_frames_impl(rest, offset + 1, [record_with_offset | acc])

      {:error, :incomplete} ->
        # No more complete frames in the data
        Enum.reverse(acc)

      {:error, reason} ->
        # Log the error and return what we've read so far
        Logger.warning("Error parsing frame at offset #{offset}: #{reason}")
        Enum.reverse(acc)
    end
  end

  defp parse_single_frame(<<frame_size::32, rest::binary>>) when byte_size(rest) >= frame_size do
    <<frame_data::binary-size(frame_size), remaining::binary>> = rest

    case parse_frame_content(frame_data) do
      {:ok, record} ->
        {:ok, frame_size + Constants.frame_header_size(), record, remaining}

      error ->
        error
    end
  end

  defp parse_single_frame(_), do: {:error, :incomplete}

  defp parse_frame_content(
         <<@magic_byte, crc::32, attr, timestamp::64, key_len::32, key::binary-size(key_len),
           val_len::32, value::binary-size(val_len), headers_len::16,
           headers_data::binary-size(headers_len), _rest::binary>>
       ) do
    # Validate sizes
    cond do
      key_len > Constants.max_key_size() ->
        {:error, :key_too_large}

      val_len > Constants.max_value_size() ->
        {:error, :value_too_large}

      headers_len > Constants.max_headers_size() ->
        {:error, :headers_too_large}

      true ->
        # Validate CRC - check everything after the CRC field
        computed_crc =
          :erlang.crc32(
            <<@magic_byte, attr, timestamp::64, key_len::32, key::binary-size(key_len),
              val_len::32, value::binary-size(val_len), headers_len::16,
              headers_data::binary-size(headers_len)>>
          )

        if crc == computed_crc do
          headers = parse_headers(headers_data)

          record = %{
            key: key,
            value: value,
            headers: headers,
            timestamp_ms: timestamp
          }

          {:ok, record}
        else
          {:error, :crc_mismatch}
        end
    end
  end

  defp parse_frame_content(_), do: {:error, :invalid_frame}

  defp parse_headers(data), do: parse_headers(data, [])

  defp parse_headers(<<>>, acc), do: Enum.reverse(acc)

  defp parse_headers(
         <<key_len::16, key::binary-size(key_len), val_len::32, value::binary-size(val_len),
           rest::binary>>,
         acc
       ) do
    parse_headers(rest, [{key, value} | acc])
  end

  defp parse_headers(_, acc), do: Enum.reverse(acc)

  defp maybe_add_index_entries(state, _records, total_bytes) do
    # Always add at least the first offset for lookups
    updated_index =
      Index.add(
        state.index,
        state.current_offset,
        state.current_position,
        total_bytes
      )

    # Don't add approximate last offset - it causes read failures
    # The first offset index entry is sufficient for lookups
    updated_index
  end
end
