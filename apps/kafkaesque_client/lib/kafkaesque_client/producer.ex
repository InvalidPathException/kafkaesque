defmodule KafkaesqueClient.Producer do
  @moduledoc """
  A Kafka-style producer for sending records to Kafkaesque topics.

  Supports both synchronous and asynchronous sends with callbacks,
  batching, and automatic retries.
  """
  use GenServer
  require Logger

  alias KafkaesqueClient.Config
  alias KafkaesqueClient.Connection.Pool
  alias KafkaesqueClient.Record.{ProducerRecord, RecordMetadata}
  alias KafkaesqueClient.Telemetry

  defstruct [
    :config,
    :pool,
    :batch,
    :batch_timer,
    :callbacks,
    :metrics,
    :flush_waiters,
    :cleanup_timer
  ]

  @type t :: %__MODULE__{
          config: map(),
          pool: atom() | pid(),
          batch: [ProducerRecord.t()],
          batch_timer: reference() | nil,
          callbacks: %{reference() => {function() | {:sync, GenServer.from()}, integer()}},
          metrics: map(),
          flush_waiters: [GenServer.from()],
          cleanup_timer: reference() | nil
        }

  @type send_callback :: (
          {:ok, RecordMetadata.t()}
          | {:error, term()}
          -> any()
        )

  # Client API

  @doc """
  Starts a new producer with the given configuration.

  ## Options

  - `:bootstrap_servers` - List of Kafkaesque servers (required)
  - `:acks` - `:none`, `:leader`, or `:all` (default: `:leader`)
  - `:batch_size` - Maximum batch size in bytes (default: 16384)
  - `:linger_ms` - Time to wait before sending incomplete batches (default: 100)
  - `:max_retries` - Maximum number of retries (default: 3)
  - `:retry_backoff_ms` - Backoff between retries (default: 100)
  - `:callback_timeout_ms` - Timeout for callbacks before cleanup (default: 30000)
  """
  def start_link(config) when is_map(config) do
    case Config.validate_producer_config(config) do
      {:ok, validated_config} ->
        GenServer.start_link(__MODULE__, validated_config)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Sends a record asynchronously with an optional callback.
  """
  @spec send(GenServer.server(), ProducerRecord.t(), send_callback() | nil) :: :ok
  def send(producer, %ProducerRecord{} = record, callback \\ nil) do
    GenServer.cast(producer, {:send, record, callback})
  end

  @doc """
  Sends a record synchronously, waiting for the result.
  """
  @spec send_sync(GenServer.server(), ProducerRecord.t(), timeout()) ::
          {:ok, RecordMetadata.t()} | {:error, term()}
  def send_sync(producer, %ProducerRecord{} = record, timeout \\ 5_000) do
    GenServer.call(producer, {:send_sync, record}, timeout)
  end

  @doc """
  Flushes any pending records in the batch synchronously.
  """
  @spec flush(GenServer.server(), timeout()) :: :ok
  def flush(producer, timeout \\ 5_000) do
    GenServer.call(producer, :flush, timeout)
  end

  @doc """
  Flushes any pending records in the batch asynchronously.
  Returns immediately without waiting for the batch to be sent.
  """
  @spec flush_async(GenServer.server()) :: :ok
  def flush_async(producer) do
    GenServer.cast(producer, :flush_async)
  end

  @doc """
  Closes the producer, flushing any pending records.
  """
  @spec close(GenServer.server(), timeout()) :: :ok
  def close(producer, timeout \\ 5_000) do
    GenServer.call(producer, :close, timeout)
  end

  @doc """
  Returns metrics about the producer.
  """
  @spec metrics(GenServer.server()) :: map()
  def metrics(producer) do
    GenServer.call(producer, :metrics)
  end

  # Server Callbacks

  @impl true
  def init(config) do
    state = %__MODULE__{
      config: config,
      pool: Pool,
      batch: [],
      batch_timer: nil,
      callbacks: %{},
      metrics: %{
        records_sent: 0,
        batches_sent: 0,
        errors: 0,
        retries: 0,
        callbacks_timeout: 0
      },
      flush_waiters: [],
      cleanup_timer: nil
    }

    # Start batch timer if linger_ms > 0
    state = maybe_start_batch_timer(state)

    # Start callback cleanup timer
    cleanup_timer = Process.send_after(self(), :cleanup_callbacks, 5_000)
    state = %{state | cleanup_timer: cleanup_timer}

    {:ok, state}
  end

  @impl true
  def handle_cast({:send, record, callback}, state) do
    ref = make_ref()

    new_callbacks =
      if callback do
        Map.put(state.callbacks, ref, {callback, System.monotonic_time(:millisecond)})
      else
        state.callbacks
      end

    # Add record to batch with its reference
    record_with_ref = {record, ref}
    new_batch = [record_with_ref | state.batch]

    new_state = %{state | batch: new_batch, callbacks: new_callbacks}

    # Check if batch should be sent
    if should_send_batch?(new_state) do
      {:noreply, send_batch(new_state)}
    else
      {:noreply, maybe_start_batch_timer(new_state)}
    end
  end

  def handle_cast(:flush_async, state) do
    if Enum.empty?(state.batch) do
      {:noreply, state}
    else
      {:noreply, send_batch(state)}
    end
  end

  @impl true
  def handle_call({:send_sync, record}, from, state) do
    ref = make_ref()

    # Store the caller to reply when batch is sent
    new_callbacks = Map.put(state.callbacks, ref, {{:sync, from}, System.monotonic_time(:millisecond)})

    record_with_ref = {record, ref}
    new_batch = [record_with_ref | state.batch]

    new_state = %{state | batch: new_batch, callbacks: new_callbacks}

    if should_send_batch?(new_state) do
      {:noreply, send_batch(new_state)}
    else
      {:noreply, maybe_start_batch_timer(new_state)}
    end
  end

  def handle_call(:flush, from, state) do
    if Enum.empty?(state.batch) do
      {:reply, :ok, state}
    else
      new_waiters = [from | state.flush_waiters]
      new_state = send_batch(%{state | flush_waiters: new_waiters})
      {:noreply, new_state}
    end
  end

  def handle_call(:close, _from, state) do
    # Send any pending batch
    final_state = if Enum.empty?(state.batch), do: state, else: send_batch(state)

    # Cancel timers if active
    if final_state.batch_timer do
      Process.cancel_timer(final_state.batch_timer)
    end
    if final_state.cleanup_timer do
      Process.cancel_timer(final_state.cleanup_timer)
    end

    {:stop, :normal, :ok, final_state}
  end

  def handle_call(:metrics, _from, state) do
    {:reply, state.metrics, state}
  end

  @impl true
  def handle_info(:send_batch, state) do
    {:noreply, send_batch(%{state | batch_timer: nil})}
  end

  def handle_info(:cleanup_callbacks, state) do
    now = System.monotonic_time(:millisecond)
    timeout_ms = Map.get(state.config, :callback_timeout_ms, 30_000)

    {expired, active} =
      Map.split_with(state.callbacks, fn {_ref, {_cb, timestamp}} ->
        now - timestamp > timeout_ms
      end)

    # Execute expired callbacks with timeout error
    Enum.each(expired, fn {_ref, {callback, _timestamp}} ->
      execute_callback_with_error(callback, :timeout)
    end)

    # Update metrics
    new_metrics = if map_size(expired) > 0 do
      %{state.metrics | callbacks_timeout: state.metrics.callbacks_timeout + map_size(expired)}
    else
      state.metrics
    end

    # Schedule next cleanup
    cleanup_timer = Process.send_after(self(), :cleanup_callbacks, 5_000)

    {:noreply, %{state | callbacks: active, cleanup_timer: cleanup_timer, metrics: new_metrics}}
  end

  # Private Functions

  defp should_send_batch?(state) do
    batch_size = calculate_batch_size(state.batch)
    batch_size >= state.config.batch_size
  end

  defp calculate_batch_size(batch) do
    Enum.reduce(batch, 0, fn {record, _ref}, acc ->
      acc + byte_size(record.value) + byte_size(record.key || <<>>)
    end)
  end

  defp maybe_start_batch_timer(%{batch_timer: nil, batch: [_ | _]} = state) do
    linger_ms = Map.get(state.config, :linger_ms, 100)

    if linger_ms > 0 do
      timer = Process.send_after(self(), :send_batch, linger_ms)
      %{state | batch_timer: timer}
    else
      state
    end
  end

  defp maybe_start_batch_timer(state), do: state

  defp send_batch(%{batch: []} = state), do: state

  defp send_batch(state) do
    # Cancel any existing timer
    if state.batch_timer do
      Process.cancel_timer(state.batch_timer)
    end

    # Group records by topic
    batches_by_topic =
      state.batch
      |> Enum.reverse()  # Preserve order
      |> Enum.group_by(fn {record, _ref} -> record.topic end)

    # Send each topic batch
    Enum.each(batches_by_topic, fn {topic, records_with_refs} ->
      send_topic_batch(state, topic, records_with_refs)
    end)

    # Reply to flush waiters
    Enum.each(state.flush_waiters, fn from ->
      GenServer.reply(from, :ok)
    end)

    # Update metrics
    new_metrics = %{
      state.metrics
      | records_sent: state.metrics.records_sent + length(state.batch),
        batches_sent: state.metrics.batches_sent + map_size(batches_by_topic)
    }

    %{state | batch: [], batch_timer: nil, flush_waiters: [], metrics: new_metrics}
  end

  defp send_topic_batch(state, topic, records_with_refs) do
    {records, refs} = Enum.unzip(records_with_refs)

    # Convert to protobuf records
    proto_records = Enum.map(records, &ProducerRecord.to_proto/1)

    # Build produce request
    request = %Kafkaesque.ProduceRequest{
      topic: topic,
      partition: 0,
      records: proto_records,
      acks: map_acks(state.config.acks),
      max_batch_bytes: state.config.batch_size
    }

    # Execute with retries
    result = execute_with_retry(state, {:produce, request}, state.config.max_retries)

    # Process callbacks
    case result do
      {:ok, response} ->
        handle_success(state, response, records, refs)

      {:error, reason} ->
        handle_error(state, reason, refs)
    end
  end

  defp execute_with_retry(state, request, retries_left, attempt \\ 1) do
    start_time = System.monotonic_time(:nanosecond)

    case Pool.execute(state.pool, request) do
      {:ok, response} ->
        duration = System.monotonic_time(:nanosecond) - start_time
        Telemetry.record_produce_latency(duration)
        {:ok, response}

      {:error, reason} when retries_left > 0 ->
        Logger.warning("Producer request failed (attempt #{attempt}): #{inspect(reason)}, retrying...")

        base_backoff = state.config.retry_backoff_ms * attempt
        jitter = :rand.uniform(Kernel.max(div(base_backoff, 4), 1))
        backoff = base_backoff + jitter
        Process.sleep(backoff)

        execute_with_retry(state, request, retries_left - 1, attempt + 1)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_success(state, response, records, refs) do
    metadata_list =
      records
      |> Enum.with_index()
      |> Enum.map(fn {_record, idx} ->
        RecordMetadata.from_produce_response(response, idx)
      end)

    # Execute callbacks
    Enum.zip([refs, metadata_list])
    |> Enum.each(fn {ref, metadata} ->
      case Map.get(state.callbacks, ref) do
        nil -> :ok

        {{:sync, from}, _timestamp} ->
          GenServer.reply(from, {:ok, metadata})

        {callback, _timestamp} when is_function(callback, 1) ->
          Task.start(fn -> callback.({:ok, metadata}) end)
      end
    end)
  end

  defp handle_error(state, reason, refs) do
    # Execute error callbacks
    Enum.each(refs, fn ref ->
      case Map.get(state.callbacks, ref) do
        nil -> :ok

        {{:sync, from}, _timestamp} ->
          GenServer.reply(from, {:error, reason})

        {callback, _timestamp} when is_function(callback, 1) ->
          Task.start(fn -> callback.({:error, reason}) end)
      end
    end)
  end

  defp execute_callback_with_error({:sync, from}, reason) do
    GenServer.reply(from, {:error, reason})
  end

  defp execute_callback_with_error(callback, reason) when is_function(callback, 1) do
    Task.start(fn -> callback.({:error, reason}) end)
  end

  defp map_acks(:none), do: :ACKS_NONE
  defp map_acks(:leader), do: :ACKS_LEADER
  defp map_acks(:all), do: :ACKS_LEADER
  defp map_acks(_), do: :ACKS_LEADER
end
