defmodule Kafkaesque.Pipeline.Producer do
  @moduledoc """
  GenStage producer that accepts messages for a topic partition.
  Manages backpressure and queuing of incoming messages.
  """

  use GenStage
  require Logger

  alias Kafkaesque.Telemetry

  defstruct [
    :topic,
    :partition,
    :queue,
    :demand,
    :max_queue_size,
    :dropped_messages
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
      {:ok, :enqueued} when acks == :none ->
        {:ok,
         %{
           topic: topic,
           partition: partition,
           count: length(messages)
         }}

      {:ok, :enqueued} ->
        {:ok,
         %{
           topic: topic,
           partition: partition,
           count: length(messages),
           base_offset: 0
         }}

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
      dropped_messages: 0
    }

    Logger.info(
      "Producer started for #{topic}/#{partition} with max queue size #{max_queue_size}"
    )

    {:producer, state}
  end

  @impl true
  def handle_call({:produce, messages, _acks}, _from, state) do
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
      # Add messages to queue with metadata
      timestamped_messages =
        Enum.map(messages, fn msg ->
          Map.merge(msg, %{
            topic: msg[:topic] || state.topic,
            partition: msg[:partition] || state.partition,
            timestamp_ms: msg[:timestamp_ms] || System.system_time(:millisecond),
            producer_timestamp: System.monotonic_time(:microsecond)
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

      new_state = %{
        state
        | queue: updated_queue,
          demand: updated_demand
      }

      {:reply, {:ok, :enqueued}, events, new_state}
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
        {"State", %{state | queue: :queue.len(state.queue)}}
      ]
    ]
  end
end
