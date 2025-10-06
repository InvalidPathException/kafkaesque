defmodule KafkaesqueClient.Consumer do
  @moduledoc """
  A Kafka-style consumer for reading records from Kafkaesque topics.

  Supports consumer groups, automatic offset management, and the poll() pattern.
  """
  use GenServer
  require Logger

  alias Kafkaesque.DescribeTopicRequest
  alias Kafkaesque.Kafkaesque.Stub
  alias KafkaesqueClient.Config
  alias KafkaesqueClient.Connection.Pool
  alias KafkaesqueClient.ConsumerGroup
  alias KafkaesqueClient.Record.{ConsumerRecord, OffsetAndMetadata, TopicPartition}

  @metadata_ttl 60_000

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
    :metadata_cache,
    :stream_tasks
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
          metadata_cache: %{optional(String.t()) => %{metadata: map(), fetched_at: integer()}},
          stream_tasks: %{TopicPartition.t() => Task.t()}
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
      metadata_cache: %{},
      stream_tasks: %{}
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
    case ensure_topics_metadata(state, topics) do
      {:ok, metadata_list, state_with_meta} ->
        new_subscriptions = MapSet.union(state_with_meta.subscriptions, MapSet.new(topics))

        assignment_set =
          metadata_list
          |> Enum.reduce(MapSet.new(state_with_meta.assignments), fn metadata, acc ->
            Enum.reduce(metadata.partition_ids, acc, fn partition, set ->
              MapSet.put(set, TopicPartition.new(metadata.name, partition))
            end)
          end)

        new_assignments =
          assignment_set
          |> Enum.to_list()
          |> Enum.sort_by(fn %TopicPartition{topic: topic, partition: partition} -> {topic, partition} end)

        new_position =
          Enum.reduce(new_assignments, state_with_meta.position, fn tp, pos ->
            Map.put_new(pos, tp, determine_initial_offset(state_with_meta, tp))
          end)

        new_state = %{
          state_with_meta
          | subscriptions: new_subscriptions,
            assignments: new_assignments,
            position: new_position
        }

        new_state = ensure_partition_streams(new_state)

        {:reply, :ok, new_state}

      {:error, {:topic_not_found, topic}, new_state} ->
        {:reply, {:error, "Topic #{topic} does not exist"}, new_state}

      {:error, {:rpc_error, reason}, new_state} ->
        {:reply, {:error, reason}, new_state}

      {:error, reason, new_state} ->
        {:reply, {:error, reason}, new_state}
    end
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
    new_state = restart_partition_stream(new_state, tp)

    {:reply, :ok, new_state}
  end

  def handle_call({:pause, _partitions}, _from, state) do
    new_state = stop_all_streams(state)
    {:reply, :ok, new_state}
  end

  def handle_call({:resume, _partitions}, _from, state) do
    new_state = ensure_partition_streams(state)
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

    # Stop active streams
    final_state = stop_all_streams(state)

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

  def handle_info({:partition_batch, _tp, batch}, state) do
    records = ConsumerRecord.from_batch(batch)
    new_buffer = Enum.reduce(records, state.buffer, &:queue.in(&1, &2))

    {:noreply, %{state | buffer: new_buffer}}
  end

  def handle_info({:partition_stream_error, tp, reason}, state) do
    Logger.error("Stream error on #{inspect(tp)}: #{inspect(reason)}")

    new_metrics = %{
      state.metrics
      | errors: state.metrics.errors + 1
    }

    new_state = %{state | metrics: new_metrics}
    new_state = restart_partition_stream(new_state, tp)

    {:noreply, new_state}
  end

  def handle_info({:partition_stream_ended, tp, _reason}, state) do
    new_state = restart_partition_stream(state, tp)
    {:noreply, new_state}
  end

  # Private Functions

  defp ensure_topics_metadata(state, topics) do
    Enum.reduce_while(topics, {:ok, [], state}, fn topic, {:ok, metadata_acc, acc_state} ->
      case get_topic_metadata(acc_state, topic) do
        {:ok, metadata, updated_state} ->
          {:cont, {:ok, [metadata | metadata_acc], updated_state}}

        {:error, reason, updated_state} ->
          {:halt, {:error, reason, updated_state}}
      end
    end)
    |> case do
      {:ok, metadata_list, final_state} -> {:ok, Enum.reverse(metadata_list), final_state}
      {:error, reason, final_state} -> {:error, reason, final_state}
    end
  end

  defp get_topic_metadata(state, topic) do
    now = System.system_time(:millisecond)

    case Map.get(state.metadata_cache, topic) do
      %{metadata: metadata, fetched_at: fetched_at} when now - fetched_at <= @metadata_ttl ->
        {:ok, metadata, state}

      _ ->
        request = %DescribeTopicRequest{topic: topic}

        case Pool.execute(state.pool, {:describe_topic, request}) do
          {:ok, response} ->
            metadata = build_metadata(response)
            cache_entry = %{metadata: metadata, fetched_at: now}
            new_state = %{state | metadata_cache: Map.put(state.metadata_cache, topic, cache_entry)}
            {:ok, metadata, new_state}

          {:error, %GRPC.RPCError{status: 5}} ->
            {:error, {:topic_not_found, topic}, state}

          {:error, reason} ->
            {:error, {:rpc_error, reason}, state}
        end
    end
  end

  defp build_metadata(response) do
    partition_ids =
      response.partition_infos
      |> Enum.map(& &1.partition)
      |> Enum.sort()

    %{
      name: response.topic,
      partitions: response.partitions,
      partition_ids: partition_ids,
      retention_hours: response.retention_hours,
      created_at_ms: response.created_at_ms
    }
  end

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

  defp ensure_partition_streams(%{assignments: []} = state) do
    stop_all_streams(state)
  end

  defp ensure_partition_streams(state) do
    desired_set = MapSet.new(state.assignments)

    state =
      Enum.reduce(state.assignments, state, fn tp, acc ->
        maybe_start_partition_stream(acc, tp)
      end)

    stop_streams_not_in(state, desired_set)
  end

  defp maybe_start_partition_stream(state, tp) do
    if Map.has_key?(state.stream_tasks, tp) do
      state
    else
      start_partition_stream(state, tp)
    end
  end

  defp start_partition_stream(state, %TopicPartition{topic: topic, partition: partition} = tp) do
    offset = Map.get(state.position, tp, determine_initial_offset(state, tp))
    actual_offset = resolve_start_offset(state, tp, offset)

    request = %Kafkaesque.ConsumeRequest{
      topic: topic,
      partition: partition,
      group: state.group_id,
      offset: actual_offset,
      max_bytes: 1_000_000,
      max_wait_ms: state.config.fetch_max_wait_ms,
      auto_commit: false
    }

    Logger.info("Starting consumer stream for #{topic}/#{partition} at offset #{actual_offset}")

    parent = self()

    task =
      Task.async(fn ->
        case Pool.get_channel(state.pool) do
          {:ok, channel} ->
            case Stub.consume(channel, request, timeout: :infinity) do
              {:ok, stream} -> stream_loop(stream, parent, tp)
              {:error, reason} -> send(parent, {:partition_stream_error, tp, reason})
            end

          {:error, reason} ->
            send(parent, {:partition_stream_error, tp, reason})
        end
      end)

    %{state | stream_tasks: Map.put(state.stream_tasks, tp, task)}
  end

  defp resolve_start_offset(state, %TopicPartition{topic: topic, partition: partition}, offset) do
    case offset do
      :latest ->
        case get_latest_offset(state, topic, partition) do
          {:ok, latest} -> latest
          {:error, reason} ->
            Logger.error("Failed to get latest offset for #{topic}/#{partition}: #{inspect(reason)}")
            0
        end

      value when is_integer(value) -> value
    end
  end

  defp stream_loop(stream, parent, tp) do
    taken = stream |> Stream.take(1) |> Enum.to_list()

    case taken do
      [{:ok, batch}] ->
        send(parent, {:partition_batch, tp, batch})
        stream_loop(stream, parent, tp)

      [{:error, reason}] ->
        send(parent, {:partition_stream_error, tp, reason})

      [batch] when is_map(batch) ->
        send(parent, {:partition_batch, tp, batch})
        stream_loop(stream, parent, tp)

      [] ->
        send(parent, {:partition_stream_ended, tp, :normal})

      other ->
        Logger.warning("Unexpected stream result for #{inspect(tp)}: #{inspect(other)}")
        send(parent, {:partition_stream_ended, tp, :unexpected})
    end
  catch
    kind, error ->
      Logger.error("Stream loop caught #{kind} for #{inspect(tp)}: #{inspect(error)}")
      send(parent, {:partition_stream_error, tp, error})
  end

  defp restart_partition_stream(state, tp) do
    if Enum.any?(state.assignments, &(&1 == tp)) do
      state
      |> stop_partition_stream(tp)
      |> maybe_start_partition_stream(tp)
    else
      stop_partition_stream(state, tp)
    end
  end

  defp stop_partition_stream(state, tp) do
    case Map.pop(state.stream_tasks, tp) do
      {nil, tasks} ->
        %{state | stream_tasks: tasks}

      {task, tasks} ->
        Task.shutdown(task, :brutal_kill)
        %{state | stream_tasks: tasks}
    end
  end

  defp stop_streams_not_in(state, desired_set) do
    stream_tasks =
      Enum.reduce(state.stream_tasks, %{}, fn {tp, task}, acc ->
        if MapSet.member?(desired_set, tp) do
          Map.put(acc, tp, task)
        else
          Task.shutdown(task, :brutal_kill)
          acc
        end
      end)

    %{state | stream_tasks: stream_tasks}
  end

  defp stop_all_streams(state) do
    Enum.each(state.stream_tasks, fn {_tp, task} -> Task.shutdown(task, :brutal_kill) end)
    %{state | stream_tasks: %{}}
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
