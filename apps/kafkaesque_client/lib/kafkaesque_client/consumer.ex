defmodule KafkaesqueClient.Consumer do
  @moduledoc """
  A Kafka-style consumer for reading records from Kafkaesque topics.

  Supports consumer groups, automatic offset management, and the poll() pattern.
  """
  use GenServer
  require Logger

  alias KafkaesqueClient.Config
  alias KafkaesqueClient.Connection.Pool
  alias KafkaesqueClient.Consumer.StreamCoordinator
  alias KafkaesqueClient.ConsumerGroup
  alias KafkaesqueClient.Record.{ConsumerRecord, OffsetAndMetadata, TopicPartition}

  defstruct [
    :config,
    :pool,
    :group_id,
    :subscriptions,
    :assignments,
    :position,
    :buffer,
    :consumer_group,
    :last_commit_time,
    :metrics,
    :closed,
    :auto_commit_timer,
    :stream_coordinator
  ]

  @type t :: %__MODULE__{
          config: map(),
          pool: atom() | pid(),
          group_id: String.t(),
          subscriptions: MapSet.t(String.t()),
          assignments: [TopicPartition.t()],
          position: %{TopicPartition.t() => integer()},
          buffer: :queue.queue(),
          consumer_group: pid() | nil,
          last_commit_time: integer(),
          metrics: map(),
          closed: boolean(),
          auto_commit_timer: reference() | nil,
          stream_coordinator: pid() | nil
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
  Unsubscribes from the given topics and tears down partition streams.
  """
  @spec unsubscribe(GenServer.server(), [String.t()]) :: :ok
  def unsubscribe(consumer, topics) when is_list(topics) do
    GenServer.call(consumer, {:unsubscribe, topics})
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
      buffer: :queue.new(),
      consumer_group: nil,
      last_commit_time: System.monotonic_time(:millisecond),
      metrics: %{
        records_consumed: 0,
        polls: 0,
        commits: 0,
        errors: 0
      },
      closed: false,
      auto_commit_timer: nil,
      stream_coordinator: nil
    }

    # Start consumer group coordinator if we have a group
    state =
      if config.group_id && config.group_id != "" do
        {:ok, group_pid} = ConsumerGroup.start_link(config)
        %{state | consumer_group: group_pid}
      else
        state
      end

    {:ok, coordinator} =
      StreamCoordinator.start_link(
        pool: Pool,
        config: config,
        group_id: config.group_id,
        owner: self()
      )

    state = %{state | stream_coordinator: coordinator}

    # Start auto-commit timer if enabled
    state =
      if config.enable_auto_commit do
        timer_ref = Process.send_after(self(), :auto_commit, config.auto_commit_interval_ms)
        %{state | auto_commit_timer: timer_ref}
      else
        state
      end

    {:ok, state}
  end

  @impl true
  def handle_call({:subscribe, topics}, _from, state) do
    case StreamCoordinator.subscribe(state.stream_coordinator, topics) do
      {:ok, metadata_list} ->
        new_subscriptions = MapSet.union(state.subscriptions, MapSet.new(topics))

        assignment_set =
          metadata_list
          |> Enum.reduce(MapSet.new(state.assignments), fn metadata, acc ->
            Enum.reduce(metadata.partition_ids, acc, fn partition, set ->
              MapSet.put(set, TopicPartition.new(metadata.name, partition))
            end)
          end)

        new_assignments =
          assignment_set
          |> Enum.to_list()
          |> Enum.sort_by(fn %TopicPartition{topic: topic, partition: partition} ->
            {topic, partition}
          end)

        new_position =
          Enum.reduce(new_assignments, state.position, fn tp, pos ->
            Map.put_new(pos, tp, determine_initial_offset(state, tp))
          end)

        :ok = StreamCoordinator.assign(state.stream_coordinator, new_assignments, new_position)
        :ok = StreamCoordinator.update_positions(state.stream_coordinator, new_position)

        new_state = %{
          state
          | subscriptions: new_subscriptions,
            assignments: new_assignments,
            position: new_position
        }

        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:unsubscribe, topics}, _from, state) do
    :ok = StreamCoordinator.unsubscribe(state.stream_coordinator, topics)

    topic_set = MapSet.new(topics)

    new_subscriptions = MapSet.difference(state.subscriptions, topic_set)

    new_assignments =
      state.assignments
      |> Enum.reject(fn %TopicPartition{topic: topic} -> MapSet.member?(topic_set, topic) end)

    new_position =
      Enum.reduce(state.position, %{}, fn {tp, offset}, acc ->
        if MapSet.member?(topic_set, tp.topic) do
          acc
        else
          Map.put(acc, tp, offset)
        end
      end)

    new_buffer =
      state.buffer
      |> :queue.to_list()
      |> Enum.reject(fn %ConsumerRecord{topic: topic} -> MapSet.member?(topic_set, topic) end)
      |> Enum.reduce(:queue.new(), fn record, acc -> :queue.in(record, acc) end)

    {:reply, :ok,
     %{
       state
       | subscriptions: new_subscriptions,
         assignments: new_assignments,
         position: new_position,
         buffer: new_buffer
     }}
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

    :ok = StreamCoordinator.update_positions(state.stream_coordinator, new_position)

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
    :ok = StreamCoordinator.seek(state.stream_coordinator, tp, offset)
    :ok = StreamCoordinator.update_positions(state.stream_coordinator, new_position)
    {:reply, :ok, %{state | position: new_position}}
  end

  def handle_call({:pause, partitions}, _from, state) do
    :ok = StreamCoordinator.pause(state.stream_coordinator, partitions)
    {:reply, :ok, state}
  end

  def handle_call({:resume, partitions}, _from, state) do
    :ok = StreamCoordinator.resume(state.stream_coordinator, partitions)
    {:reply, :ok, state}
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

    # Stop active streams
    if state.stream_coordinator do
      StreamCoordinator.stop(state.stream_coordinator)
    end

    final_state = %{state | stream_coordinator: nil, buffer: :queue.new()}

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

    {:noreply,
     %{
       state
       | last_commit_time: System.monotonic_time(:millisecond),
         auto_commit_timer: timer_ref
     }}
  end

  def handle_info({:stream_records, records}, state) do
    new_buffer = Enum.reduce(records, state.buffer, &:queue.in(&1, &2))
    {:noreply, %{state | buffer: new_buffer}}
  end

  # Private Functions

  defp determine_initial_offset(state, %TopicPartition{} = tp) do
    # Check if we have a committed offset for this consumer group
    if state.consumer_group do
      case ConsumerGroup.get_committed_offset(state.consumer_group, tp) do
        {:ok, offset} ->
          # Resume from next offset
          offset + 1

        {:error, :not_found} ->
          case state.config.auto_offset_reset do
            :earliest -> 0
            # Use atom, will be resolved later
            :latest -> :latest
          end
      end
    else
      case state.config.auto_offset_reset do
        :earliest -> 0
        # Use atom, will be resolved later
        :latest -> :latest
      end
    end
  end

  defp poll_records_with_buffer(state, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    max_records = state.config.max_poll_records

    poll_from_buffer_with_state(state.buffer, [], max_records, deadline)
  end

  defp poll_from_buffer_with_state(buffer, acc, max_records, _deadline)
       when length(acc) >= max_records do
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
          # Commit last consumed offset
          offset: offset - 1
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
end
