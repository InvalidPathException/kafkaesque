defmodule Kafkaesque.Storage.SingleFile do
  @moduledoc """
  Single append-only file storage per topic/partition.
  Implements binary framing with magic byte, attributes, and timestamps.
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

    case Index.lookup(state.index, start_offset) do
      {:ok, file_position} ->
        {:ok, data} = :file.pread(state.file, file_position, max_bytes)

        records = parse_frames(data, start_offset, max_bytes)

        duration_ms =
          System.convert_time_unit(
            System.monotonic_time() - start_time,
            :native,
            :millisecond
          )

        Telemetry.storage_read(state.topic, state.partition, byte_size(data), duration_ms)

        {:reply, {:ok, records}, state}

      :not_found ->
        {:reply, {:error, :offset_out_of_range}, state}
    end
  end

  @impl true
  def handle_call(:get_offsets, _from, state) do
    offsets = %{
      # In production, would track retention_start_offset
      earliest: 0,
      latest: state.current_offset
    }

    {:reply, {:ok, offsets}, state}
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
        {:ok, data} = :file.pread(file, 0, size)
        {offset, position, updated_index} = rebuild_from_data(data, index, 0, 0, 0)
        {offset, position, updated_index}

      {:error, :enoent} ->
        # File doesn't exist yet
        {0, 0, index}

      _ ->
        # Any other case
        {0, 0, index}
    end
  end

  defp rebuild_from_data(<<>>, index, offset, position, _) do
    {offset, position, index}
  end

  defp rebuild_from_data(data, index, offset, position, bytes_since_index) do
    case parse_single_frame(data) do
      {:ok, frame_size, _record, rest} ->
        # Always index the first record, then sparse index
        if offset == 0 or rem(offset, Constants.index_interval()) == 0 or
             bytes_since_index >= Constants.index_bytes_threshold() do
          updated_index = Index.add(index, offset, position, frame_size)

          rebuild_from_data(
            rest,
            updated_index,
            offset + 1,
            position + frame_size + Constants.frame_header_size(),
            0
          )
        else
          rebuild_from_data(
            rest,
            index,
            offset + 1,
            position + frame_size + Constants.frame_header_size(),
            bytes_since_index + frame_size
          )
        end

      {:error, :incomplete} ->
        # Corrupted tail, truncate
        Logger.warning("Truncating corrupted tail at offset #{offset}")
        {offset, position, index}
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

  defp parse_frames(data, start_offset, max_bytes) do
    parse_frames(data, start_offset, max_bytes, 0, [])
  end

  defp parse_frames(<<>>, _offset, _max_bytes, _bytes_read, acc) do
    Enum.reverse(acc)
  end

  defp parse_frames(_data, _offset, max_bytes, bytes_read, acc) when bytes_read >= max_bytes do
    Enum.reverse(acc)
  end

  defp parse_frames(data, offset, max_bytes, bytes_read, acc) do
    case parse_single_frame(data) do
      {:ok, frame_size, record, rest} ->
        parse_frames(rest, offset + 1, max_bytes, bytes_read + frame_size, [record | acc])

      {:error, :incomplete} ->
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

  defp maybe_add_index_entries(state, records, total_bytes) do
    # Always add at least the first offset for lookups
    updated_index =
      Index.add(
        state.index,
        state.current_offset,
        state.current_position,
        total_bytes
      )

    if length(records) >= Constants.index_interval() or
         total_bytes >= Constants.index_bytes_threshold() do
      last_offset = state.current_offset + length(records) - 1

      Index.add(
        updated_index,
        last_offset,
        # Approximate
        state.current_position + total_bytes - 100,
        100
      )
    else
      updated_index
    end
  end
end
