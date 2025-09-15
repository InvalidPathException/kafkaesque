defmodule Kafkaesque.Pipeline.BatchingConsumer do
  @moduledoc """
  GenStage consumer that batches messages before writing to storage.
  Provides time-based and size-based batching similar to Kafka.
  """

  use GenStage
  require Logger

  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Telemetry

  defstruct [
    :topic,
    :partition,
    :batch,
    :batch_size,
    :batch_timeout,
    :batch_timer,
    :last_batch_time
  ]

  @default_batch_size Application.compile_env(:kafkaesque_core, :max_batch_size, 500)
  @default_batch_timeout Application.compile_env(:kafkaesque_core, :batch_timeout, 5) * 1000

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    GenStage.start_link(__MODULE__, opts, name: via_tuple(topic, partition))
  end

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    # Convert batch_timeout to milliseconds if provided in seconds
    batch_timeout =
      case Keyword.get(opts, :batch_timeout) do
        nil -> @default_batch_timeout
        # Assume seconds if < 100
        timeout when timeout < 100 -> timeout * 1000
        # Already in milliseconds
        timeout -> timeout
      end

    state = %__MODULE__{
      topic: topic,
      partition: partition,
      batch: [],
      batch_size: batch_size,
      batch_timeout: batch_timeout,
      batch_timer: nil,
      last_batch_time: System.monotonic_time(:millisecond)
    }

    # Subscribe to producer
    producer = {:via, Registry, {Kafkaesque.TopicRegistry, {:producer, topic, partition}}}

    Logger.info(
      "BatchingConsumer started for #{topic}/#{partition} " <>
        "batch_size=#{batch_size}, timeout=#{batch_timeout}ms"
    )

    {:consumer, state,
     subscribe_to: [
       {producer, min_demand: opts[:min_demand] || 5, max_demand: opts[:max_demand] || batch_size}
     ]}
  end

  @impl true
  def handle_events(events, _from, state) do
    # Add events to batch
    new_batch = state.batch ++ events
    batch_count = length(new_batch)

    Logger.debug(
      "BatchingConsumer #{state.topic}/#{state.partition} received #{length(events)} events, " <>
        "batch size now #{batch_count}"
    )

    cond do
      # Batch is full, write immediately
      batch_count >= state.batch_size ->
        {to_write, remaining} = Enum.split(new_batch, state.batch_size)
        write_batch(state, to_write)

        # Cancel timer since we just wrote
        cancel_batch_timer(state.batch_timer)

        # Start new timer if we have remaining messages
        new_timer =
          if length(remaining) > 0 do
            schedule_batch_timeout(state.batch_timeout)
          else
            nil
          end

        {:noreply, [],
         %{
           state
           | batch: remaining,
             batch_timer: new_timer,
             last_batch_time: System.monotonic_time(:millisecond)
         }}

      # First message in batch, start timer
      state.batch == [] ->
        timer = schedule_batch_timeout(state.batch_timeout)
        {:noreply, [], %{state | batch: new_batch, batch_timer: timer}}

      # Accumulating messages
      true ->
        {:noreply, [], %{state | batch: new_batch}}
    end
  end

  @impl true
  def handle_info(:batch_timeout, state) do
    if length(state.batch) > 0 do
      Logger.debug(
        "BatchingConsumer #{state.topic}/#{state.partition} timeout, " <>
          "writing #{length(state.batch)} messages"
      )

      write_batch(state, state.batch)

      {:noreply, [],
       %{
         state
         | batch: [],
           batch_timer: nil,
           last_batch_time: System.monotonic_time(:millisecond)
       }}
    else
      {:noreply, [], %{state | batch_timer: nil}}
    end
  end

  @impl true
  def terminate(_reason, state) do
    # Flush any remaining messages
    if length(state.batch) > 0 do
      Logger.info(
        "BatchingConsumer #{state.topic}/#{state.partition} terminating, " <>
          "flushing #{length(state.batch)} messages"
      )

      write_batch(state, state.batch)
    end

    :ok
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:batching_consumer, topic, partition}}}
  end

  defp schedule_batch_timeout(timeout) do
    Process.send_after(self(), :batch_timeout, timeout)
  end

  defp cancel_batch_timer(nil), do: :ok

  defp cancel_batch_timer(timer) do
    Process.cancel_timer(timer)
    # Flush any pending timeout message
    receive do
      :batch_timeout -> :ok
    after
      0 -> :ok
    end
  end

  defp write_batch(state, messages) do
    start_time = System.monotonic_time()

    case SingleFile.append(state.topic, state.partition, messages) do
      {:ok, result} ->
        duration_ms =
          System.convert_time_unit(
            System.monotonic_time() - start_time,
            :native,
            :millisecond
          )

        # Calculate batch latency
        batch_latency =
          System.monotonic_time(:millisecond) - state.last_batch_time

        # Emit telemetry for each message
        Enum.each(messages, fn msg ->
          Telemetry.message_produced(%{
            topic: state.topic,
            partition: state.partition,
            bytes: byte_size(msg[:value] || <<>>)
          })
        end)

        # Emit batch telemetry
        :telemetry.execute(
          [:kafkaesque, :batching_consumer, :batch_written],
          %{
            count: length(messages),
            duration_ms: duration_ms,
            batch_latency_ms: batch_latency,
            bytes:
              Enum.reduce(messages, 0, fn m, acc ->
                acc + byte_size(m[:value] || <<>>)
              end)
          },
          %{
            topic: state.topic,
            partition: state.partition,
            base_offset: result[:base_offset]
          }
        )

        Logger.debug("BatchingConsumer wrote #{length(messages)} messages in #{duration_ms}ms")

        :ok

      {:error, reason} ->
        Logger.error("BatchingConsumer failed to write batch: #{inspect(reason)}")

        # Emit error telemetry
        :telemetry.execute(
          [:kafkaesque, :batching_consumer, :batch_failed],
          %{count: length(messages)},
          %{topic: state.topic, partition: state.partition, reason: reason}
        )

        {:error, reason}
    end
  end
end
