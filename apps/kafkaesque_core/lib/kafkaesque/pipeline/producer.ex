defmodule Kafkaesque.Pipeline.Producer do
  @moduledoc """
  GenStage producer that accepts messages for a topic partition.
  Manages backpressure and queuing of incoming messages.
  """

  use GenStage
  require Logger

  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Telemetry

  defstruct [
    :topic,
    :partition,
    :queue,
    :demand,
    :max_queue_size,
    :dropped_messages,
    :pending_acks
  ]

  @max_queue_size Application.compile_env(
                    :kafkaesque_core,
                    :max_queue_size,
                    10_000
                  )

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    GenStage.start_link(__MODULE__, opts, name: via_tuple(topic, partition))
  end

  @doc """
  Produce messages to the topic partition.
  Returns {:ok, result} or {:error, :backpressure} if queue is full.
  """
  def produce(topic, partition, messages, opts \\ []) do
    acks = Keyword.get(opts, :acks, :leader)
    timeout = Keyword.get(opts, :timeout, 5000)

    case GenStage.call(via_tuple(topic, partition), {:produce, messages, acks}, timeout) do
      {:ok, result} when acks == :none ->
        # For acks=none, don't include base_offset
        {:ok, Map.delete(result, :base_offset)}

      {:ok, result} ->
        {:ok, result}

      error ->
        error
    end
  end

  @doc """
  Get current queue statistics.
  """
  def get_stats(topic, partition) do
    GenStage.call(via_tuple(topic, partition), :get_stats)
  end

  # GenStage callbacks

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    max_queue_size = Keyword.get(opts, :max_queue_size, @max_queue_size)

    state = %__MODULE__{
      topic: topic,
      partition: partition,
      queue: :queue.new(),
      demand: 0,
      max_queue_size: max_queue_size,
      dropped_messages: 0,
      pending_acks: %{}
    }

    Logger.info(
      "Producer started for #{topic}/#{partition} with max queue size #{max_queue_size}"
    )

    {:producer, state}
  end

  @impl true
  def handle_call({:produce, messages, acks}, from, state) do
    current_size = :queue.len(state.queue)
    new_size = current_size + length(messages)

    if new_size > state.max_queue_size do
      # Emit telemetry for dropped messages
      Telemetry.execute(
        [:kafkaesque, :producer, :backpressure],
        %{dropped: length(messages)},
        %{topic: state.topic, partition: state.partition}
      )

      new_state = %{state | dropped_messages: state.dropped_messages + length(messages)}
      {:reply, {:error, :backpressure}, [], new_state}
    else
      # Generate a request ID for tracking
      request_id = :erlang.unique_integer([:positive])

      # Add messages to queue with metadata
      timestamped_messages =
        Enum.map(messages, fn msg ->
          Map.merge(msg, %{
            topic: msg[:topic] || state.topic,
            partition: msg[:partition] || state.partition,
            timestamp_ms: msg[:timestamp_ms] || System.system_time(:millisecond),
            producer_timestamp: System.monotonic_time(:microsecond),
            request_id: request_id
          })
        end)

      new_queue =
        Enum.reduce(timestamped_messages, state.queue, fn msg, q ->
          :queue.in(msg, q)
        end)

      # Emit telemetry
      Telemetry.execute(
        [:kafkaesque, :producer, :enqueued],
        %{count: length(messages), queue_size: :queue.len(new_queue)},
        %{topic: state.topic, partition: state.partition}
      )

      # Try to dispatch if we have pending demand
      {events, updated_queue, updated_demand} =
        dispatch_events(new_queue, state.demand, [])

      # Store pending ack info if we need to wait for actual offset
      new_pending_acks =
        if acks != :none do
          Map.put(state.pending_acks, request_id, %{
            from: from,
            acks: acks,
            count: length(messages),
            timestamp: System.monotonic_time(:millisecond)
          })
        else
          state.pending_acks
        end

      new_state = %{
        state
        | queue: updated_queue,
          demand: updated_demand,
          pending_acks: new_pending_acks
      }

      if acks == :none do
        # For acks=none, reply immediately without offset
        result = %{
          topic: state.topic,
          partition: state.partition,
          count: length(messages)
        }

        {:reply, {:ok, result}, events, new_state}
      else
        # For acks=leader, we'll reply when we get confirmation
        # Start a timer to check for write completion
        Process.send_after(self(), {:check_write_completion, request_id}, 50)
        {:noreply, events, new_state}
      end
    end
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats = %{
      topic: state.topic,
      partition: state.partition,
      queue_size: :queue.len(state.queue),
      max_queue_size: state.max_queue_size,
      pending_demand: state.demand,
      dropped_messages: state.dropped_messages,
      utilization: Float.round(:queue.len(state.queue) * 100.0 / state.max_queue_size, 2)
    }

    {:reply, stats, [], state}
  end

  @impl true
  def handle_demand(incoming_demand, state) do
    # Add to existing demand
    total_demand = state.demand + incoming_demand

    # Try to fulfill demand from queue
    {events, updated_queue, remaining_demand} =
      dispatch_events(state.queue, total_demand, [])

    if length(events) > 0 do
      Logger.debug(
        "Producer #{state.topic}/#{state.partition} dispatched #{length(events)} events, " <>
          "remaining demand: #{remaining_demand}"
      )

      # Emit telemetry
      Telemetry.execute(
        [:kafkaesque, :producer, :dispatched],
        %{count: length(events), remaining_demand: remaining_demand},
        %{topic: state.topic, partition: state.partition}
      )
    end

    new_state = %{
      state
      | queue: updated_queue,
        demand: remaining_demand
    }

    {:noreply, events, new_state}
  end

  @impl true
  def handle_info({:check_write_completion, request_id}, state) do
    case Map.get(state.pending_acks, request_id) do
      nil ->
        # Already handled
        {:noreply, [], state}

      %{from: from, count: count, timestamp: start_time} ->
        # Check if messages were written by examining storage offsets
        case SingleFile.get_offsets(state.topic, state.partition) do
          {:ok, %{latest: latest, earliest: _earliest}} ->
            # Calculate base offset (assuming messages were written sequentially)
            base_offset = max(0, latest - count + 1)

            # Reply with actual offsets
            result = %{
              topic: state.topic,
              partition: state.partition,
              count: count,
              base_offset: base_offset
            }

            GenStage.reply(from, {:ok, result})

            # Remove from pending acks
            new_pending_acks = Map.delete(state.pending_acks, request_id)
            {:noreply, [], %{state | pending_acks: new_pending_acks}}

          _ ->
            # Storage not ready yet or error, check again
            elapsed = System.monotonic_time(:millisecond) - start_time

            if elapsed > 5000 do
              # Timeout - reply with error
              GenStage.reply(from, {:error, :write_timeout})
              new_pending_acks = Map.delete(state.pending_acks, request_id)
              {:noreply, [], %{state | pending_acks: new_pending_acks}}
            else
              # Try again after a delay
              Process.send_after(self(), {:check_write_completion, request_id}, 100)
              {:noreply, [], state}
            end
        end
    end
  end

  @impl true
  def handle_info({:telemetry, event}, state) do
    # Handle telemetry events if needed
    Logger.debug("Producer received telemetry event: #{inspect(event)}")
    {:noreply, [], state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Producer received unexpected message: #{inspect(msg)}")
    {:noreply, [], state}
  end

  # Private functions

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:producer, topic, partition}}}
  end

  defp dispatch_events(queue, 0, events) do
    {Enum.reverse(events), queue, 0}
  end

  defp dispatch_events(queue, demand, events) when demand > 0 do
    case :queue.out(queue) do
      {{:value, event}, new_queue} ->
        dispatch_events(new_queue, demand - 1, [event | events])

      {:empty, queue} ->
        {Enum.reverse(events), queue, demand}
    end
  end

  @impl true
  def format_status(:normal, [_pdict, state]) do
    [
      data: [
        {"State",
         %{state | queue: :queue.len(state.queue), pending_acks: map_size(state.pending_acks)}}
      ]
    ]
  end
end
