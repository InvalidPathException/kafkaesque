defmodule Kafkaesque.Pipeline.MessageBuffer do
  @moduledoc """
  Message buffer that queues messages for Broadway consumption.
  Acts as a simple queue service that Broadway's producer can pull from.
  """

  use GenServer
  require Logger

  defstruct [
    :topic,
    :partition,
    :queue,
    :max_queue_size,
    :waiting_demand
  ]

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    GenServer.start_link(__MODULE__, opts, name: via_tuple(topic, partition))
  end

  @doc """
  Enqueue messages to be processed by Broadway.
  Returns {:ok, :enqueued} or {:error, :queue_full}
  """
  def enqueue(topic, partition, messages) do
    GenServer.call(via_tuple(topic, partition), {:enqueue, messages})
  end

  @doc """
  Pull messages from the buffer.
  Called by Broadway's producer when it needs messages.
  """
  def pull(topic, partition, demand) do
    GenServer.call(via_tuple(topic, partition), {:pull, demand})
  end

  @doc """
  Get current queue size.
  """
  def get_stats(topic, partition) do
    GenServer.call(via_tuple(topic, partition), :get_stats)
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:message_buffer, topic, partition}}}
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      topic: opts[:topic],
      partition: opts[:partition],
      queue: :queue.new(),
      max_queue_size: opts[:max_queue_size] || 10_000,
      waiting_demand: nil
    }

    Logger.info("MessageBuffer started for #{state.topic}/#{state.partition}")

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, messages}, _from, state) do
    current_size = :queue.len(state.queue)
    new_size = current_size + length(messages)

    if new_size > state.max_queue_size do
      {:reply, {:error, :queue_full}, state}
    else
      new_queue =
        Enum.reduce(messages, state.queue, fn msg, q ->
          :queue.in(msg, q)
        end)

      new_state = %{state | queue: new_queue}

      new_state = maybe_send_to_waiting(new_state)

      {:reply, {:ok, :enqueued}, new_state}
    end
  end

  @impl true
  def handle_call({:pull, demand}, from, state) do
    {messages, new_queue} = pull_from_queue(state.queue, demand, [])

    if length(messages) > 0 do
      {:reply, {:ok, messages}, %{state | queue: new_queue}}
    else
      {:reply, {:ok, []}, %{state | waiting_demand: {from, demand}}}
    end
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats = %{
      queue_size: :queue.len(state.queue),
      max_queue_size: state.max_queue_size,
      has_waiting_demand: state.waiting_demand != nil
    }

    {:reply, stats, state}
  end

  defp pull_from_queue(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp pull_from_queue(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, message}, new_queue} ->
        pull_from_queue(new_queue, demand - 1, [message | acc])

      {:empty, _queue} ->
        {Enum.reverse(acc), queue}
    end
  end

  defp maybe_send_to_waiting(%{waiting_demand: nil} = state), do: state

  defp maybe_send_to_waiting(%{waiting_demand: {from, demand}} = state) do
    {messages, new_queue} = pull_from_queue(state.queue, demand, [])

    if length(messages) > 0 do
      GenServer.reply(from, {:ok, messages})
      %{state | queue: new_queue, waiting_demand: nil}
    else
      state
    end
  end
end
