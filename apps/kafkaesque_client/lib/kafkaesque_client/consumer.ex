defmodule KafkaesqueClient.Consumer do
  @moduledoc """
  A Kafka-style consumer for reading records from Kafkaesque topics.

  Supports consumer groups, automatic offset management, and the poll() pattern.
  """
  use GenServer
  require Logger

  alias Kafkaesque.Kafkaesque.Stub
  alias KafkaesqueClient.Config
  alias KafkaesqueClient.Connection.Pool
  alias KafkaesqueClient.ConsumerGroup
  alias KafkaesqueClient.Record.{ConsumerRecord, OffsetAndMetadata, TopicPartition}

  defstruct [
    :config,
    :pool,
    :group_id,
    :subscriptions,
    :assignments,
    :position,
    :stream,
    :buffer,
    :stream_task,
    :consumer_group,
    :last_commit_time,
    :metrics,
    :closed,
    :auto_commit_timer
  ]

  @type t :: %__MODULE__{
          config: map(),
          pool: atom() | pid(),
          group_id: String.t(),
          subscriptions: MapSet.t(String.t()),
          assignments: [TopicPartition.t()],
          position: %{TopicPartition.t() => integer()},
          stream: GenServer.server() | nil,
          buffer: :queue.queue(),
          stream_task: Task.t() | nil,
          consumer_group: pid() | nil,
          last_commit_time: integer(),
          metrics: map(),
          closed: boolean(),
          auto_commit_timer: reference() | nil
        }

  # Client API

  @doc """
  Starts a new consumer with the given configuration.

  ## Options

  - `:bootstrap_servers` - List of Kafkaesque servers (required)
  - `:group_id` - Consumer group ID (required)
  - `:auto_offset_reset` - `:earliest` or `:latest` (default: `:latest`)
  - `:enable_auto_commit` - Enable auto-commit (default: `true`)
  - `:auto_commit_interval_ms` - Auto-commit interval (default: 5000)
  - `:max_poll_records` - Maximum records per poll (default: 500)
  - `:session_timeout_ms` - Session timeout (default: 30000)
  """
  def start_link(config) when is_map(config) do
    case Config.validate_consumer_config(config) do
      {:ok, validated_config} ->
        GenServer.start_link(__MODULE__, validated_config)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Subscribes to one or more topics.
  """
  @spec subscribe(GenServer.server(), [String.t()]) :: :ok | {:error, term()}
  def subscribe(consumer, topics) when is_list(topics) do
    GenServer.call(consumer, {:subscribe, topics})
  end

  @doc """
  Polls for records from the subscribed topics.

  Returns a list of ConsumerRecord structs.
  """
  @spec poll(GenServer.server(), timeout()) :: [ConsumerRecord.t()]
  def poll(consumer, timeout \\ 1_000) do
    GenServer.call(consumer, {:poll, timeout}, timeout + 1_000)
  end

  @doc """
  Manually commits the current offsets.
  """
  @spec commit_sync(GenServer.server()) :: :ok | {:error, term()}
  def commit_sync(consumer) do
    GenServer.call(consumer, :commit_sync)
  end

  @doc """
  Commits specific offsets for topic partitions.
  """
  @spec commit_sync(GenServer.server(), %{TopicPartition.t() => OffsetAndMetadata.t()}) ::
          :ok | {:error, term()}
  def commit_sync(consumer, offsets) when is_map(offsets) do
    GenServer.call(consumer, {:commit_sync, offsets})
  end

  @doc """
  Seeks to a specific offset for a topic partition.
  """
  @spec seek(GenServer.server(), TopicPartition.t(), integer()) :: :ok
  def seek(consumer, %TopicPartition{} = tp, offset) do
    GenServer.call(consumer, {:seek, tp, offset})
  end

  @doc """
  Pauses consumption from specific topic partitions.
  """
  @spec pause(GenServer.server(), [TopicPartition.t()]) :: :ok
  def pause(consumer, partitions) do
    GenServer.call(consumer, {:pause, partitions})
  end

  @doc """
  Resumes consumption from paused topic partitions.
  """
  @spec resume(GenServer.server(), [TopicPartition.t()]) :: :ok
  def resume(consumer, partitions) do
    GenServer.call(consumer, {:resume, partitions})
  end

  @doc """
  Closes the consumer, committing offsets if auto-commit is enabled.
  """
  @spec close(GenServer.server()) :: :ok
  def close(consumer) do
    GenServer.call(consumer, :close)
  end

  @doc """
  Returns metrics about the consumer.
  """
  @spec metrics(GenServer.server()) :: map()
  def metrics(consumer) do
    GenServer.call(consumer, :metrics)
  end

  # Server Callbacks

  @impl true
  def init(config) do
    state = %__MODULE__{
      config: config,
      pool: Pool,
      group_id: config.group_id,
      subscriptions: MapSet.new(),
      assignments: [],
      position: %{},
      stream: nil,
      buffer: :queue.new(),
      stream_task: nil,
      consumer_group: nil,
      last_commit_time: System.monotonic_time(:millisecond),
      metrics: %{
        records_consumed: 0,
        polls: 0,
        commits: 0,
        errors: 0
      },
      closed: false,
      auto_commit_timer: nil
    }

    # Start consumer group coordinator if we have a group
    state =
      if config.group_id && config.group_id != "" do
        {:ok, group_pid} = ConsumerGroup.start_link(config)
        %{state | consumer_group: group_pid}
      else
        state
      end

    # Start auto-commit timer if enabled
    state = if config.enable_auto_commit do
      timer_ref = Process.send_after(self(), :auto_commit, config.auto_commit_interval_ms)
      %{state | auto_commit_timer: timer_ref}
    else
      state
    end

    {:ok, state}
  end

  @impl true
  def handle_call({:subscribe, topics}, _from, state) do
    new_subscriptions = MapSet.union(state.subscriptions, MapSet.new(topics))

    # For MVP, assign all topics with partition 0
    new_assignments =
      Enum.map(topics, fn topic ->
        TopicPartition.new(topic, 0)
      end)

    # Initialize positions
    new_position =
      Enum.reduce(new_assignments, state.position, fn tp, pos ->
        initial = determine_initial_offset(state, tp)
        Map.put_new(pos, tp, initial)
      end)

    new_state = %{
      state
      | subscriptions: new_subscriptions,
        assignments: new_assignments,
        position: new_position
    }

    # Start streaming if not already started
    new_state = maybe_start_stream(new_state)

    {:reply, :ok, new_state}
  end

  def handle_call({:poll, timeout}, _from, state) do
    {records, new_buffer} = poll_records_with_buffer(state, timeout)

    # Update position based on consumed records
    new_position = update_position(state.position, records)

    new_metrics = %{
      state.metrics
      | records_consumed: state.metrics.records_consumed + length(records),
        polls: state.metrics.polls + 1
    }

    {:reply, records, %{state | buffer: new_buffer, position: new_position, metrics: new_metrics}}
  end

  def handle_call(:commit_sync, _from, state) do
    result = commit_current_offsets(state)

    new_metrics = %{
      state.metrics
      | commits: state.metrics.commits + 1
    }

    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call({:commit_sync, offsets}, _from, state) do
    result = commit_specific_offsets(state, offsets)

    new_metrics = %{
      state.metrics
      | commits: state.metrics.commits + 1
    }

    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call({:seek, tp, offset}, _from, state) do
    new_position = Map.put(state.position, tp, offset)

    # Restart stream to seek
    new_state = %{state | position: new_position}
    new_state = restart_stream(new_state)

    {:reply, :ok, new_state}
  end

  def handle_call({:pause, _partitions}, _from, state) do
    new_state = stop_stream(state)
    {:reply, :ok, new_state}
  end

  def handle_call({:resume, _partitions}, _from, state) do
    new_state = maybe_start_stream(state)
    {:reply, :ok, new_state}
  end

  def handle_call(:close, _from, state) do
    # Mark as closed to prevent further operations
    state = %{state | closed: true}

    # Cancel auto-commit timer if active
    if state.auto_commit_timer do
      Process.cancel_timer(state.auto_commit_timer)
    end

    # Commit offsets if auto-commit is enabled
    if state.config.enable_auto_commit do
      commit_current_offsets(state)
    end

    # Stop the stream
    final_state = stop_stream(state)

    {:stop, :normal, :ok, final_state}
  end

  def handle_call(:metrics, _from, state) do
    {:reply, state.metrics, state}
  end

  @impl true
  def handle_info(:auto_commit, %{closed: true} = state) do
    # Consumer is closed, ignore auto-commit
    {:noreply, state}
  end

  def handle_info(:auto_commit, state) do
    if should_auto_commit?(state) do
      commit_current_offsets(state)
    end

    # Schedule next auto-commit
    timer_ref = Process.send_after(self(), :auto_commit, state.config.auto_commit_interval_ms)

    {:noreply, %{state | last_commit_time: System.monotonic_time(:millisecond), auto_commit_timer: timer_ref}}
  end

  def handle_info({:batch_received, batch}, state) do
    # Add batch to buffer
    records = ConsumerRecord.from_batch(batch)
    new_buffer = Enum.reduce(records, state.buffer, &:queue.in(&1, &2))

    # Track batch receipt (use trace level for detailed logging)

    {:noreply, %{state | buffer: new_buffer}}
  end

  def handle_info({:stream_error, reason}, state) do
    Logger.error("Stream error: #{inspect(reason)}")

    new_metrics = %{
      state.metrics
      | errors: state.metrics.errors + 1
    }

    # Restart stream after error
    new_state = %{state | metrics: new_metrics}
    new_state = restart_stream(new_state)

    {:noreply, new_state}
  end

  def handle_info({:stream_ended, _reason}, state) do
    # Stream ended normally, restart if still subscribed
    new_state =
      if MapSet.size(state.subscriptions) > 0 do
        maybe_start_stream(state)
      else
        state
      end

    {:noreply, new_state}
  end

  # Private Functions

  defp determine_initial_offset(state, %TopicPartition{} = tp) do
    # Check if we have a committed offset for this consumer group
    if state.consumer_group do
      case ConsumerGroup.get_committed_offset(state.consumer_group, tp) do
        {:ok, offset} ->
          offset + 1  # Resume from next offset

        {:error, :not_found} ->
          case state.config.auto_offset_reset do
            :earliest -> 0
            :latest -> :latest  # Use atom, will be resolved later
          end
      end
    else
      case state.config.auto_offset_reset do
        :earliest -> 0
        :latest -> :latest  # Use atom, will be resolved later
      end
    end
  end

  defp maybe_start_stream(%{stream_task: nil} = state) do
    if length(state.assignments) > 0 do
      start_stream(state)
    else
      state
    end
  end

  defp maybe_start_stream(state), do: state

  defp start_stream(state) do
    case List.first(state.assignments) do
      nil ->
        state

      %TopicPartition{topic: topic, partition: partition} = tp ->
        offset = Map.get(state.position, tp, 0)

        # Handle :latest by getting the latest offset from server
        actual_offset =
          case offset do
            :latest ->
              # Get latest offset from server (which is next write position)
              # So we start from there to get only new messages
              case get_latest_offset(state, topic, partition) do
                {:ok, latest} -> latest
                {:error, reason} ->
                  Logger.error("Failed to get latest offset for #{topic}/#{partition}: #{inspect(reason)}")
                  0
              end
            o when is_integer(o) -> o
          end

        # Create consume request
        request = %Kafkaesque.ConsumeRequest{
          topic: topic,
          partition: partition,
          group: state.group_id,
          offset: actual_offset,
          max_bytes: 1_000_000,
          max_wait_ms: state.config.fetch_max_wait_ms,
          auto_commit: false  # We handle commits manually
        }

        Logger.info("Starting consumer stream for #{topic}/#{partition} at offset #{actual_offset}")

        # Start streaming task
        parent = self()

        task =
          Task.async(fn ->
            # Stream task starting for topic/partition

            # Get channel directly and create stream in this process
            case Pool.get_channel(state.pool) do
              {:ok, channel} ->
                # Create stream directly with the channel
                case Stub.consume(channel, request, timeout: :infinity) do
                  {:ok, stream} ->
                    stream_loop(stream, parent)

                  {:error, reason} ->
                    Logger.error("Failed to create stream: #{inspect(reason)}")
                    send(parent, {:stream_error, reason})
                end

              {:error, reason} ->
                Logger.error("Failed to get channel: #{inspect(reason)}")
                send(parent, {:stream_error, reason})
            end
          end)

        %{state | stream_task: task}
    end
  end

  defp stream_loop(stream, parent) do
    # Try using Stream.take with timeout
    taken = stream
      |> Stream.take(1)
      |> Enum.to_list()

    # Process stream result

    case taken do
      [{:ok, batch}] ->
        send(parent, {:batch_received, batch})
        stream_loop(stream, parent)

      [{:error, reason}] ->
        Logger.error("Stream error: #{inspect(reason)}")
        send(parent, {:stream_error, reason})

      [batch] when is_map(batch) ->
        send(parent, {:batch_received, batch})
        stream_loop(stream, parent)

      [] ->
        send(parent, {:stream_ended, :normal})

      other ->
        Logger.warning("Unexpected stream result: #{inspect(other)}")
        send(parent, {:stream_ended, :unexpected})
    end
  catch
    kind, error ->
      Logger.error("Stream loop caught #{kind}: #{inspect(error)}")
      send(parent, {:stream_error, error})
  end

  defp stop_stream(%{stream_task: nil} = state), do: state

  defp stop_stream(%{stream_task: task} = state) do
    Task.shutdown(task, :brutal_kill)
    %{state | stream_task: nil, stream: nil}
  end

  defp restart_stream(state) do
    state
    |> stop_stream()
    |> maybe_start_stream()
  end

  defp poll_records_with_buffer(state, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    max_records = state.config.max_poll_records

    poll_from_buffer_with_state(state.buffer, [], max_records, deadline)
  end

  defp poll_from_buffer_with_state(buffer, acc, max_records, _deadline) when length(acc) >= max_records do
    {Enum.reverse(acc), buffer}
  end

  defp poll_from_buffer_with_state(buffer, acc, max_records, deadline) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      {Enum.reverse(acc), buffer}
    else
      case :queue.out(buffer) do
        {{:value, record}, new_buffer} ->
          poll_from_buffer_with_state(new_buffer, [record | acc], max_records, deadline)

        {:empty, _} ->
          # Wait for more records or timeout
          remaining = deadline - now
          if remaining > 0 do
            receive do
              {:batch_received, _batch} ->
                # New batch arrived, continue polling
                poll_from_buffer_with_state(buffer, acc, max_records, deadline)
            after
              min(100, remaining) ->
                {Enum.reverse(acc), buffer}
            end
          else
            {Enum.reverse(acc), buffer}
          end
      end
    end
  end

  defp update_position(position, records) do
    Enum.reduce(records, position, fn record, pos ->
      tp = TopicPartition.new(record.topic, record.partition)
      Map.put(pos, tp, record.offset + 1)
    end)
  end

  defp should_auto_commit?(state) do
    state.config.enable_auto_commit &&
      not state.closed &&
      MapSet.size(state.subscriptions) > 0 &&
      map_size(state.position) > 0
  end

  defp commit_current_offsets(state) do
    offsets =
      Enum.map(state.position, fn {tp, offset} ->
        %Kafkaesque.CommitOffsetsRequest.Offset{
          topic: tp.topic,
          partition: tp.partition,
          offset: offset - 1  # Commit last consumed offset
        }
      end)

    request = %Kafkaesque.CommitOffsetsRequest{
      group: state.group_id,
      offsets: offsets
    }

    case Pool.execute(state.pool, {:commit_offsets, request}) do
      {:ok, _response} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp commit_specific_offsets(state, offsets_map) do
    offsets =
      Enum.map(offsets_map, fn {tp, om} ->
        %Kafkaesque.CommitOffsetsRequest.Offset{
          topic: tp.topic,
          partition: tp.partition,
          offset: om.offset
        }
      end)

    request = %Kafkaesque.CommitOffsetsRequest{
      group: state.group_id,
      offsets: offsets
    }

    case Pool.execute(state.pool, {:commit_offsets, request}) do
      {:ok, _response} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_latest_offset(state, topic, partition) do
    request = %Kafkaesque.GetOffsetsRequest{
      topic: topic,
      partition: partition
    }

    case Pool.execute(state.pool, {:get_offsets, request}) do
      {:ok, response} ->
        {:ok, response.latest}
      {:error, reason} ->
        Logger.warning("Failed to get latest offset for #{topic}/#{partition}: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
