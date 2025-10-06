defmodule KafkaesqueClient.Consumer.StreamCoordinator do
  @moduledoc """
  Manages topic metadata, per-partition consume streams, and buffering on behalf
  of `KafkaesqueClient.Consumer`.

  The coordinator is responsible for:
  * Fetching & caching `DescribeTopic` metadata.
  * Starting one streaming task per topic-partition and restarting on failures.
  * Tracking the latest offset per partition so restarts continue from the right
    position.
  * Delivering batches to the owning consumer via `{:stream_records, records}`
    messages.
  """

  use GenServer
  require Logger

  alias Kafkaesque.DescribeTopicRequest
  alias Kafkaesque.Kafkaesque.Stub
  alias KafkaesqueClient.Connection.Pool
  alias KafkaesqueClient.Record.{ConsumerRecord, TopicPartition}

  @metadata_ttl 60_000
  @retry_backoff_ms 250
  @max_retry_backoff_ms 5_000

  defstruct [
    :pool,
    :config,
    :group_id,
    :owner,
    :stub,
    metadata_cache: %{},
    subscriptions: MapSet.new(),
    assignments: MapSet.new(),
    positions: %{},
    stream_tasks: %{},
    paused: %{},
    restart_attempts: %{},
    restart_timers: %{}
  ]

  ## Public API -------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc """
  Fetches metadata for the given topics and updates the coordinator's metadata
  cache. Returns the metadata so the consumer can update its assignment state.
  """
  @spec subscribe(GenServer.server(), [String.t()]) :: {:ok, [map()]} | {:error, term()}
  def subscribe(coordinator, topics) when is_list(topics) do
    GenServer.call(coordinator, {:subscribe, topics})
  end

  @doc """
  Removes the topics from the subscription set and tears down any active
  streams for their partitions.
  """
  @spec unsubscribe(GenServer.server(), [String.t()]) :: :ok
  def unsubscribe(coordinator, topics) when is_list(topics) do
    GenServer.call(coordinator, {:unsubscribe, topics})
  end

  @doc """
  Assigns the given topic-partitions and starts/stops streams as needed. The
  `positions` map should contain the next offset to consume for each partition.
  """
  @spec assign(GenServer.server(), [TopicPartition.t()], map()) :: :ok | {:error, term()}
  def assign(coordinator, partitions, positions) do
    GenServer.call(coordinator, {:assign, partitions, positions})
  end

  @doc """
  Updates the latest offsets tracked by the coordinator after the consumer has
  processed records.
  """
  @spec update_positions(GenServer.server(), map()) :: :ok
  def update_positions(coordinator, positions) do
    GenServer.cast(coordinator, {:update_positions, positions})
  end

  @doc """
  Seeks a specific partition to the provided offset.
  """
  @spec seek(GenServer.server(), TopicPartition.t(), integer()) :: :ok | {:error, term()}
  def seek(coordinator, tp, offset) do
    GenServer.call(coordinator, {:seek, tp, offset})
  end

  @doc """
  Pauses the given partitions (streams are stopped but assignments retained).
  """
  @spec pause(GenServer.server(), [TopicPartition.t()], keyword()) :: :ok
  def pause(coordinator, partitions, opts \\ []) do
    reason = Keyword.get(opts, :reason, :manual)
    GenServer.call(coordinator, {:pause, partitions, reason})
  end

  @doc """
  Resumes the given partitions using the stored offsets.
  """
  @spec resume(GenServer.server(), [TopicPartition.t()], keyword()) :: :ok | {:error, term()}
  def resume(coordinator, partitions, opts \\ []) do
    reason = Keyword.get(opts, :reason, :manual)
    GenServer.call(coordinator, {:resume, partitions, reason})
  end

  @doc """
  Convenience helper to pause partitions for coordinator-managed backpressure.
  """
  @spec pause_for_backpressure(GenServer.server(), [TopicPartition.t()]) :: :ok
  def pause_for_backpressure(coordinator, partitions) do
    pause(coordinator, partitions, reason: :backpressure)
  end

  @doc """
  Convenience helper to resume partitions previously paused for backpressure.
  """
  @spec resume_from_backpressure(GenServer.server(), [TopicPartition.t()]) ::
          :ok | {:error, term()}
  def resume_from_backpressure(coordinator, partitions) do
    resume(coordinator, partitions, reason: :backpressure)
  end

  @doc """
  Applies rebalance changes by adding new partitions, revoking removed ones, and
  updating starting offsets.
  """
  @spec rebalance(GenServer.server(), [TopicPartition.t()], [TopicPartition.t()], map()) :: :ok
  def rebalance(coordinator, to_assign, to_revoke, positions) do
    GenServer.call(coordinator, {:rebalance, to_assign, to_revoke, positions})
  end

  @doc """
  Stops all partition streams and clears coordinator state.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(coordinator) do
    GenServer.cast(coordinator, :stop)
  end

  ## GenServer callbacks ----------------------------------------------------

  @impl true
  def init(opts) do
    pool = Keyword.fetch!(opts, :pool)
    config = Keyword.fetch!(opts, :config)
    group_id = Keyword.get(opts, :group_id, "")
    owner = Keyword.fetch!(opts, :owner)
    stub = Keyword.get(opts, :stub, Stub)

    state = %__MODULE__{
      pool: pool,
      config: config,
      group_id: group_id,
      owner: owner,
      stub: stub
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:subscribe, topics}, _from, state) do
    case ensure_topics_metadata(state, topics) do
      {:ok, metadata_list, new_state} ->
        updated_state = %{
          new_state
          | subscriptions: MapSet.union(new_state.subscriptions, MapSet.new(topics))
        }

        {:reply, {:ok, metadata_list}, updated_state}

      {:error, reason, new_state} ->
        {:reply, {:error, reason}, new_state}
    end
  end

  @impl true
  def handle_call({:assign, partitions, positions}, _from, state) do
    assignments = MapSet.new(partitions)

    merged_positions =
      state.positions
      |> Map.merge(positions)
      |> Map.take(MapSet.to_list(assignments))

    state = stop_streams_not_in(state, assignments)

    state =
      state
      |> Map.put(:assignments, assignments)
      |> prune_paused(assignments)

    state =
      Enum.reduce(assignments, state, fn tp, acc ->
        maybe_start_partition_stream(acc, tp, merged_positions)
      end)

    {:reply, :ok, %{state | positions: merged_positions}}
  end

  def handle_call({:unsubscribe, topics}, _from, state) do
    topic_set = MapSet.new(topics)

    partitions_to_drop =
      state.assignments
      |> Enum.filter(fn %TopicPartition{topic: topic} -> MapSet.member?(topic_set, topic) end)

    state = Enum.reduce(partitions_to_drop, state, &stop_partition_stream(&2, &1))

    new_assignments = MapSet.difference(state.assignments, MapSet.new(partitions_to_drop))
    new_positions = Map.drop(state.positions, partitions_to_drop)
    new_paused = Map.drop(state.paused, partitions_to_drop)

    new_state = %{
      state
      | subscriptions: MapSet.difference(state.subscriptions, topic_set),
        assignments: new_assignments,
        positions: new_positions,
        paused: new_paused,
        metadata_cache: Map.drop(state.metadata_cache, topics)
    }

    {:reply, :ok, new_state}
  end

  def handle_call({:seek, tp, offset}, _from, state) do
    positions = Map.put(state.positions, tp, offset)
    state = state |> stop_partition_stream(tp) |> maybe_start_partition_stream(tp, positions)
    {:reply, :ok, %{state | positions: positions}}
  end

  def handle_call({:pause, partitions, reason}, _from, state) do
    state =
      Enum.reduce(partitions, state, fn tp, acc ->
        acc
        |> stop_partition_stream(tp)
        |> add_pause(tp, reason)
      end)

    {:reply, :ok, state}
  end

  def handle_call({:resume, partitions, reason}, _from, state) do
    state =
      Enum.reduce(partitions, state, fn tp, acc ->
        remove_pause(acc, tp, reason)
      end)

    state =
      Enum.reduce(partitions, state, fn tp, acc ->
        if paused?(acc, tp) do
          acc
        else
          maybe_start_partition_stream(acc, tp, acc.positions)
        end
      end)

    {:reply, :ok, state}
  end

  def handle_call({:rebalance, to_assign, to_revoke, positions}, _from, state) do
    additions = Enum.uniq(to_assign)
    removals = Enum.uniq(to_revoke)

    add_set = MapSet.new(additions)
    remove_set = MapSet.new(removals)

    state = Enum.reduce(removals, state, &stop_partition_stream(&2, &1))

    assignments =
      state.assignments
      |> MapSet.difference(remove_set)
      |> MapSet.union(add_set)

    positions =
      state.positions
      |> Map.drop(removals)
      |> Map.merge(positions)
      |> Map.take(MapSet.to_list(assignments))

    state =
      state
      |> drop_pauses(removals)
      |> Map.put(:assignments, assignments)
      |> Map.put(:positions, positions)
      |> prune_paused(assignments)

    state =
      Enum.reduce(additions, state, fn tp, acc ->
        maybe_start_partition_stream(acc, tp, positions)
      end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:poll, _max_records, _timeout}, _from, state) do
    {:reply, [], state}
  end

  @impl true
  def handle_cast({:update_positions, positions}, state) do
    merged =
      state.positions
      |> Map.merge(positions)
      |> Map.take(MapSet.to_list(state.assignments))

    {:noreply, %{state | positions: merged}}
  end

  def handle_cast(:stop, state) do
    Enum.each(state.stream_tasks, fn {_tp, task} -> Task.shutdown(task, :brutal_kill) end)
    Enum.each(state.restart_timers, fn {_tp, ref} -> Process.cancel_timer(ref) end)

    cleared_state = %{
      state
      | stream_tasks: %{},
        assignments: MapSet.new(),
        positions: %{},
        subscriptions: MapSet.new(),
        paused: %{},
        restart_attempts: %{},
        restart_timers: %{}
    }

    {:noreply, cleared_state}
  end

  @impl true
  def handle_info({:stream_batch, tp, batch}, state) do
    records = ConsumerRecord.from_batch(batch)

    case records do
      [] ->
        {:noreply, state}

      _ ->
        send(state.owner, {:stream_records, records})
        last_offset = List.last(records).offset + 1
        state = %{state | positions: Map.put(state.positions, tp, last_offset)}
        {:noreply, state}
    end
  end

  def handle_info({:stream_error, tp, reason}, state) do
    Logger.error("Stream error on #{inspect(tp)}: #{inspect(reason)}")
    state = restart_partition_stream(state, tp)
    {:noreply, state}
  end

  def handle_info({:stream_ended, tp, reason}, state) do
    Logger.info("Stream ended for #{inspect(tp)}: #{inspect(reason)}")
    state = restart_partition_stream(state, tp)
    {:noreply, state}
  end

  def handle_info({:restart_partition, tp}, state) do
    state = cancel_restart_timer(state, tp, keep_attempt: true)

    state =
      if MapSet.member?(state.assignments, tp) and not paused?(state, tp) do
        maybe_start_partition_stream(state, tp, state.positions)
      else
        cancel_restart_timer(state, tp)
      end

    {:noreply, state}
  end

  ## Internal helpers -------------------------------------------------------

  defp ensure_topics_metadata(state, topics) do
    Enum.reduce_while(topics, {:ok, [], state}, fn topic, {:ok, acc, acc_state} ->
      case get_topic_metadata(acc_state, topic) do
        {:ok, metadata, updated_state} -> {:cont, {:ok, [metadata | acc], updated_state}}
        {:error, reason, updated_state} -> {:halt, {:error, reason, updated_state}}
      end
    end)
    |> case do
      {:ok, metadata_list, new_state} -> {:ok, Enum.reverse(metadata_list), new_state}
      {:error, reason, new_state} -> {:error, reason, new_state}
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

            new_state = %{
              state
              | metadata_cache: Map.put(state.metadata_cache, topic, cache_entry)
            }

            {:ok, metadata, new_state}

          {:error, reason} ->
            {:error, reason, state}
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
      partition_ids: partition_ids,
      partitions: response.partitions,
      retention_hours: response.retention_hours,
      created_at_ms: response.created_at_ms
    }
  end

  defp maybe_start_partition_stream(state, tp, positions) do
    cond do
      not MapSet.member?(state.assignments, tp) -> state
      paused?(state, tp) -> state
      Map.has_key?(state.stream_tasks, tp) -> state
      true -> start_partition_stream(state, tp, positions)
    end
  end

  defp start_partition_stream(
         state,
         %TopicPartition{topic: topic, partition: partition} = tp,
         positions
       ) do
    offset = Map.get(positions, tp, 0)
    actual_offset = resolve_start_offset(state, tp, offset)

    state = cancel_restart_timer(state, tp)

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
            case state.stub.consume(channel, request, timeout: :infinity) do
              {:ok, stream} -> stream_loop(stream, parent, tp)
              {:error, reason} -> send(parent, {:stream_error, tp, reason})
            end

          {:error, reason} ->
            send(parent, {:stream_error, tp, reason})
        end
      end)

    %{state | stream_tasks: Map.put(state.stream_tasks, tp, task)}
  end

  defp stream_loop(stream, parent, tp) do
    taken = stream |> Stream.take(1) |> Enum.to_list()

    case taken do
      [{:ok, batch}] ->
        send(parent, {:stream_batch, tp, batch})
        stream_loop(stream, parent, tp)

      [{:error, reason}] ->
        send(parent, {:stream_error, tp, reason})

      [batch] when is_map(batch) ->
        send(parent, {:stream_batch, tp, batch})
        stream_loop(stream, parent, tp)

      [] ->
        send(parent, {:stream_ended, tp, :normal})

      other ->
        send(parent, {:stream_ended, tp, {:unexpected, other}})
    end
  catch
    kind, error ->
      send(parent, {:stream_error, tp, {kind, error}})
  end

  defp resolve_start_offset(state, %TopicPartition{topic: topic, partition: partition}, :latest) do
    case get_latest_offset(state, topic, partition) do
      {:ok, latest} -> latest
      {:error, _} -> 0
    end
  end

  defp resolve_start_offset(_state, _tp, offset) when is_integer(offset), do: offset

  defp restart_partition_stream(state, tp) do
    state = stop_partition_stream(state, tp, keep_attempt: true)

    cond do
      paused?(state, tp) -> state
      not MapSet.member?(state.assignments, tp) -> state
      true -> schedule_restart(state, tp)
    end
  end

  defp stop_partition_stream(state, tp, opts \\ []) do
    case Map.pop(state.stream_tasks, tp) do
      {nil, tasks} ->
        cancel_restart_timer(%{state | stream_tasks: tasks}, tp, opts)

      {task, tasks} ->
        Task.shutdown(task, :brutal_kill)
        cancel_restart_timer(%{state | stream_tasks: tasks}, tp, opts)
    end
  end

  defp stop_streams_not_in(state, desired_assignments) do
    state =
      Enum.reduce(Map.keys(state.stream_tasks), state, fn tp, acc ->
        if MapSet.member?(desired_assignments, tp) do
          acc
        else
          stop_partition_stream(acc, tp)
        end
      end)

    prune_paused(state, desired_assignments)
  end

  defp get_latest_offset(state, topic, partition) do
    request = %Kafkaesque.GetOffsetsRequest{topic: topic, partition: partition}

    case Pool.execute(state.pool, {:get_offsets, request}) do
      {:ok, response} ->
        {:ok, response.latest}

      {:error, reason} ->
        Logger.warning(
          "Failed to get latest offset for #{topic}/#{partition}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp schedule_restart(state, tp) do
    attempt = Map.get(state.restart_attempts, tp, 0) + 1
    delay = backoff_delay(attempt)
    state = cancel_restart_timer(state, tp, keep_attempt: true)
    Logger.warning("Scheduling restart for #{inspect(tp)} in #{delay}ms (attempt #{attempt})")
    ref = Process.send_after(self(), {:restart_partition, tp}, delay)

    %{
      state
      | restart_attempts: Map.put(state.restart_attempts, tp, attempt),
        restart_timers: Map.put(state.restart_timers, tp, ref)
    }
  end

  defp backoff_delay(attempt) do
    delay = @retry_backoff_ms * :math.pow(2, max(attempt - 1, 0))
    min(round(delay), @max_retry_backoff_ms)
  end

  defp cancel_restart_timer(state, tp, opts \\ []) do
    keep_attempt = Keyword.get(opts, :keep_attempt, false)

    {ref, timers} = Map.pop(state.restart_timers, tp)
    if ref, do: Process.cancel_timer(ref)

    attempts =
      if keep_attempt do
        state.restart_attempts
      else
        Map.delete(state.restart_attempts, tp)
      end

    %{state | restart_timers: timers, restart_attempts: attempts}
  end

  defp paused?(state, tp), do: Map.has_key?(state.paused, tp)

  defp add_pause(state, tp, reason) do
    reasons =
      state.paused
      |> Map.get(tp, MapSet.new())
      |> MapSet.put(reason)

    %{state | paused: Map.put(state.paused, tp, reasons)}
  end

  defp remove_pause(state, tp, :all), do: %{state | paused: Map.delete(state.paused, tp)}

  defp remove_pause(state, tp, reason) do
    case Map.get(state.paused, tp) do
      nil ->
        state

      reasons ->
        updated = MapSet.delete(reasons, reason)

        if MapSet.size(updated) == 0 do
          %{state | paused: Map.delete(state.paused, tp)}
        else
          %{state | paused: Map.put(state.paused, tp, updated)}
        end
    end
  end

  defp prune_paused(state, assignments) do
    filtered =
      state.paused
      |> Enum.filter(fn {tp, _} -> MapSet.member?(assignments, tp) end)
      |> Enum.into(%{})

    %{state | paused: filtered}
  end

  defp drop_pauses(state, partitions) do
    %{state | paused: Map.drop(state.paused, partitions)}
  end
end
