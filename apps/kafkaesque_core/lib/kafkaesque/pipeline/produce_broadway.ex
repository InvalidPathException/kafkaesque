defmodule Kafkaesque.Pipeline.ProduceBroadway do
  @moduledoc """
  Broadway pipeline for producing messages to topics.
  Pulls from MessageBuffer and handles batching, compression, and backpressure.
  """

  use Broadway
  require Logger

  alias Kafkaesque.Pipeline.MessageBuffer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Telemetry

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    name = String.to_atom("broadway_#{topic}_#{partition}")

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module:
          {__MODULE__.Producer,
           [
             topic: topic,
             partition: partition
           ]},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: opts[:processor_concurrency] || 10,
          min_demand: 5,
          max_demand: 10
        ]
      ],
      batchers: [
        default: [
          batch_size: opts[:batch_size] || 100,
          batch_timeout: (opts[:batch_timeout] || 5) * 1000,
          concurrency: opts[:batcher_concurrency] || 1
        ]
      ],
      context: %{
        topic: topic,
        partition: partition
      }
    )
  end

  @doc """
  Produce messages to a topic partition.
  Messages are enqueued to MessageBuffer for Broadway processing.
  """
  def produce(topic, partition, messages, opts \\ []) do
    acks = Keyword.get(opts, :acks, :leader)

    case MessageBuffer.enqueue(topic, partition, messages) do
      {:ok, :enqueued} ->
        if acks == :none do
          {:ok,
           %{
             topic: topic,
             partition: partition,
             count: length(messages)
           }}
        else
          {:ok,
           %{
             topic: topic,
             partition: partition,
             count: length(messages),
             base_offset: 0
           }}
        end

      {:error, :queue_full} ->
        {:error, :backpressure}
    end
  end

  # Broadway callbacks

  @impl true
  def handle_message(_, message, context) do
    data = message.data

    updated_data =
      Map.merge(data, %{
        topic: data[:topic] || context.topic,
        partition: data[:partition] || context.partition,
        timestamp_ms: data[:timestamp_ms] || System.system_time(:millisecond)
      })

    message
    |> Broadway.Message.update_data(fn _ -> updated_data end)
    |> Broadway.Message.put_batcher(:default)
  end

  @impl true
  def handle_batch(:default, messages, _batch_info, context) do
    records = Enum.map(messages, & &1.data)

    Logger.debug(
      "Writing batch of #{length(records)} messages to #{context.topic}/#{context.partition}"
    )

    case SingleFile.append(context.topic, context.partition, records) do
      {:ok, result} ->
        Enum.each(records, fn record ->
          Telemetry.message_produced(%{
            topic: context.topic,
            partition: context.partition,
            bytes: byte_size(record[:value] || <<>>)
          })
        end)

        Logger.debug("Successfully wrote batch: #{inspect(result)}")

        messages

      {:error, reason} ->
        Logger.error("Failed to write batch: #{inspect(reason)}")

        Enum.map(messages, fn msg ->
          Broadway.Message.failed(msg, reason)
        end)
    end
  end

  @impl true
  def handle_failed(messages, context) do
    Enum.each(messages, fn msg ->
      Logger.error(
        "Message failed for #{context.topic}/#{context.partition}: #{inspect(msg.data)}"
      )
    end)

    messages
  end

  @doc """
  Gets metrics for the Broadway pipeline.
  """
  def get_metrics(topic, partition) do
    name = String.to_atom("broadway_#{topic}_#{partition}")

    try do
      info =
        Broadway.producer_names(name)
        |> List.first()
        |> Process.info([:message_queue_len, :memory])

      buffer_stats = MessageBuffer.get_stats(topic, partition)

      %{
        queue_length: buffer_stats.queue_size,
        memory: Keyword.get(info || [], :memory, 0)
      }
    catch
      _, _ -> %{queue_length: 0, memory: 0}
    end
  end

  @doc """
  Gracefully drains and stops the pipeline.
  """
  def drain_and_stop(topic, partition) do
    name = String.to_atom("broadway_#{topic}_#{partition}")

    try do
      Broadway.stop(name, :normal, 30_000)
    catch
      _, _ -> :ok
    end
  end

  # Producer module that pulls from MessageBuffer
  defmodule Producer do
    @moduledoc false
    use GenStage
    require Logger

    def init(opts) do
      topic = Keyword.fetch!(opts, :topic)
      partition = Keyword.fetch!(opts, :partition)

      state = %{
        topic: topic,
        partition: partition,
        demand: 0
      }

      {:producer, state}
    end

    def handle_demand(incoming_demand, state) do
      new_demand = state.demand + incoming_demand

      case MessageBuffer.pull(state.topic, state.partition, new_demand) do
        {:ok, messages} when is_list(messages) ->
          broadway_messages = Enum.map(messages, &wrap_message/1)

          fulfilled = length(broadway_messages)
          remaining_demand = new_demand - fulfilled

          {:noreply, broadway_messages, %{state | demand: remaining_demand}}

        _ ->
          {:noreply, [], %{state | demand: new_demand}}
      end
    end

    defp wrap_message(message) do
      %Broadway.Message{
        data: message,
        metadata: %{
          topic: message[:topic],
          partition: message[:partition],
          timestamp: System.system_time(:millisecond)
        },
        acknowledger: {__MODULE__, :ack_id, :ack_data}
      }
    end

    @doc false
    def ack(_ack_ref, successful, failed) do
      if length(successful) > 0 do
        Logger.debug("Broadway Producer: #{length(successful)} messages acknowledged")
      end

      if length(failed) > 0 do
        Logger.warning("Broadway Producer: #{length(failed)} messages failed")
      end

      :ok
    end
  end
end
