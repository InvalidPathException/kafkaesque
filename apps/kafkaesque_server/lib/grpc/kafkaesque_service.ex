defmodule Kafkaesque.GRPC.Service do
  @moduledoc """
  gRPC service implementation for Kafkaesque.
  Implements the Kafkaesque service defined in kafkaesque.proto.
  """

  use GRPC.Server, service: Kafkaesque.Kafkaesque.Service

  alias Kafkaesque.{
    CommitOffsetsRequest,
    CommitOffsetsResponse,
    ConsumeRequest,
    CreateTopicRequest,
    FetchedBatch,
    GetOffsetsRequest,
    GetOffsetsResponse,
    ListTopicsResponse,
    ProduceRequest,
    ProduceResponse,
    Record,
    RecordHeader,
    Topic
  }

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.LogReader
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  require Logger

  @doc """
  Creates a new topic with the specified number of partitions.
  """
  def create_topic(%CreateTopicRequest{name: name, partitions: partitions}, _stream) do
    Logger.info("gRPC: Creating topic #{name} with #{partitions} partitions")

    partitions = if partitions <= 0, do: 1, else: partitions

    case TopicSupervisor.create_topic(name, partitions) do
      {:ok, _info} ->
        %Topic{name: name, partitions: partitions}

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "Failed to create topic: #{inspect(reason)}"
    end
  end

  @doc """
  Lists all available topics.
  """
  def list_topics(_request, _stream) do
    Logger.info("gRPC: Listing topics")

    topics = TopicSupervisor.list_topics()

    topic_list =
      Enum.map(topics, fn
        {name, partitions} when is_binary(name) and is_integer(partitions) ->
          %Topic{name: name, partitions: partitions}

        %{name: name, partitions: partitions} ->
          %Topic{name: name, partitions: partitions}
      end)

    %ListTopicsResponse{topics: topic_list}
  end

  @doc """
  Produces records to a topic partition.
  """
  def produce(%ProduceRequest{} = request, _stream) do
    Logger.info("gRPC: Producing to #{request.topic}/#{request.partition}")

    # Validate topic name
    if request.topic == nil or request.topic == "" do
      raise GRPC.RPCError,
        status: :invalid_argument,
        message: "Topic name is required"
    end

    # MVP: Always use partition 0 if not specified
    partition = if request.partition < 0, do: 0, else: request.partition

    # Check if topic exists
    case TopicSupervisor.get_topic_info(request.topic) do
      {:error, :topic_not_found} ->
        raise GRPC.RPCError,
          status: :not_found,
          message: "Topic #{request.topic} does not exist"

      _ ->
        :ok
    end

    # Convert protobuf records to internal format
    messages = Enum.map(request.records, &convert_record_to_internal/1)

    # Determine acks level
    acks =
      case request.acks do
        :ACKS_NONE -> :none
        :ACKS_LEADER -> :leader
        _ -> :leader
      end

    case Producer.produce(request.topic, partition, messages, acks: acks) do
      {:ok, result} ->
        %ProduceResponse{
          topic: request.topic,
          partition: partition,
          base_offset: Map.get(result, :base_offset, 0),
          count: result.count
        }

      {:error, :backpressure} ->
        raise GRPC.RPCError,
          status: :resource_exhausted,
          message: "Producer queue is full, please retry"

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "Failed to produce: #{inspect(reason)}"
    end
  end

  @doc """
  Consumes records from a topic partition as a streaming response.
  """
  def consume(%ConsumeRequest{} = request, stream) do
    Logger.info(
      "gRPC: Consuming from #{request.topic}/#{request.partition} for group #{request.group}"
    )

    # MVP: Always use partition 0 if not specified
    partition = if request.partition < 0, do: 0, else: request.partition

    # Determine starting offset
    offset =
      case request.offset do
        -1 -> :latest
        -2 -> :earliest
        n when n >= 0 -> n
        _ -> :latest
      end

    # Get committed offset if consuming with a group
    starting_offset =
      if request.group != "" do
        case DetsOffset.fetch(request.topic, partition, request.group) do
          {:ok, committed_offset} -> committed_offset + 1
          {:error, :not_found} -> offset
          _ -> offset
        end
      else
        offset
      end

    # Start consuming loop
    consume_loop(
      stream,
      request.topic,
      partition,
      request.group,
      starting_offset,
      request.max_bytes,
      request.max_wait_ms,
      request.auto_commit
    )
  end

  @doc """
  Commits consumer group offsets.
  """
  def commit_offsets(%CommitOffsetsRequest{} = request, _stream) do
    Logger.info("gRPC: Committing offsets for group #{request.group}")

    Enum.each(request.offsets, fn offset ->
      DetsOffset.commit(offset.topic, offset.partition, request.group, offset.offset)
    end)

    %CommitOffsetsResponse{}
  end

  @doc """
  Gets the earliest and latest offsets for a topic partition.
  """
  def get_offsets(%GetOffsetsRequest{topic: topic, partition: partition}, _stream) do
    Logger.info("gRPC: Getting offsets for #{topic}/#{partition}")

    # Check if topic exists first
    {earliest, latest} =
      case TopicSupervisor.get_topic_info(topic) do
        {:ok, _} ->
          # Topic exists, try to get offsets
          case SingleFile.get_offsets(topic, partition) do
            {:ok, %{earliest: e, latest: l}} -> {e, l}
            _ -> {0, 0}
          end

        {:error, :topic_not_found} ->
          # Topic doesn't exist, return defaults
          {0, 0}
      end

    %GetOffsetsResponse{
      earliest: earliest,
      latest: latest
    }
  end

  # Private functions

  defp convert_record_to_internal(%Record{} = record) do
    headers =
      Enum.map(record.headers, fn %RecordHeader{key: k, value: v} ->
        {k, v}
      end)

    %{
      key: record.key,
      value: record.value,
      headers: headers,
      timestamp_ms:
        if(record.timestamp_ms > 0,
          do: record.timestamp_ms,
          else: System.system_time(:millisecond)
        )
    }
  end

  defp convert_record_from_internal(record) when is_map(record) do
    headers =
      Enum.map(record[:headers] || [], fn {k, v} ->
        %RecordHeader{key: k, value: v}
      end)

    %Record{
      key: record[:key] || <<>>,
      value: record[:value] || <<>>,
      headers: headers,
      timestamp_ms: record[:timestamp_ms] || 0
    }
  end

  defp consume_loop(stream, topic, partition, group, offset, max_bytes, max_wait_ms, auto_commit) do
    consume_loop_with_state(
      stream,
      topic,
      partition,
      group,
      offset,
      max_bytes,
      max_wait_ms,
      auto_commit,
      %{
        start_time: System.monotonic_time(:millisecond),
        messages_sent: 0,
        last_heartbeat: System.monotonic_time(:millisecond),
        # 5 minutes max stream duration
        max_duration_ms: 300_000
      }
    )
  end

  defp consume_loop_with_state(
         stream,
         topic,
         partition,
         group,
         offset,
         max_bytes,
         max_wait_ms,
         auto_commit,
         state
       ) do
    now = System.monotonic_time(:millisecond)
    elapsed = now - state.start_time

    # Check if stream has been running too long
    if elapsed > state.max_duration_ms do
      Logger.info(
        "Closing long-running consume stream for #{topic}/#{partition} after #{elapsed}ms"
      )

      # Send a final empty batch to signal end of stream
      final_batch = %FetchedBatch{
        topic: topic,
        partition: partition,
        high_watermark: offset,
        base_offset: offset,
        records: []
      }

      GRPC.Server.send_reply(stream, final_batch)
      :ok
    else
      # Check stream health
      if stream_healthy?(stream) do
        case LogReader.consume(topic, partition, group, offset, max_bytes, max_wait_ms) do
          {:ok, records} when records != [] ->
            # Convert internal records to protobuf format
            pb_records = Enum.map(records, &convert_record_from_internal/1)

            # Get the high watermark (latest offset)
            high_watermark =
              case SingleFile.get_offsets(topic, partition) do
                {:ok, %{latest: hw}} -> hw
                _ -> offset
              end

            # Send batch to client
            batch = %FetchedBatch{
              topic: topic,
              partition: partition,
              high_watermark: high_watermark,
              base_offset: offset,
              records: pb_records
            }

            case safe_send_reply(stream, batch) do
              :ok ->
                # Auto-commit if enabled
                last_offset = offset + length(records) - 1

                if auto_commit and group != "" do
                  DetsOffset.commit(topic, partition, group, last_offset)
                end

                # Update state and continue
                new_state = %{
                  state
                  | messages_sent: state.messages_sent + length(records),
                    last_heartbeat: now
                }

                # Continue consuming from the next offset
                consume_loop_with_state(
                  stream,
                  topic,
                  partition,
                  group,
                  last_offset + 1,
                  max_bytes,
                  max_wait_ms,
                  auto_commit,
                  new_state
                )

              {:error, reason} ->
                Logger.warning("Failed to send batch to client: #{inspect(reason)}")
                :ok
            end

          {:ok, []} ->
            # No records available
            # Send heartbeat if needed
            new_state =
              if now - state.last_heartbeat > 30_000 do
                # Send empty batch as heartbeat
                heartbeat = %FetchedBatch{
                  topic: topic,
                  partition: partition,
                  high_watermark: offset,
                  base_offset: offset,
                  records: []
                }

                safe_send_reply(stream, heartbeat)
                %{state | last_heartbeat: now}
              else
                state
              end

            # Wait briefly and retry
            Process.sleep(min(100, max_wait_ms))

            consume_loop_with_state(
              stream,
              topic,
              partition,
              group,
              offset,
              max_bytes,
              max_wait_ms,
              auto_commit,
              new_state
            )

          {:error, reason} ->
            Logger.error("Consume error: #{inspect(reason)}")

            raise GRPC.RPCError,
              status: :internal,
              message: "Failed to consume: #{inspect(reason)}"
        end
      else
        Logger.warning("Stream appears unhealthy, closing consume loop for #{topic}/#{partition}")
        :ok
      end
    end
  end

  defp stream_healthy?(stream) do
    # Check if the stream process is still alive
    # TODO: might want more sophisticated health checks
    Process.alive?(stream.adapter_payload.pid)
  rescue
    # If we can't check, assume it's healthy
    _ -> true
  end

  defp safe_send_reply(stream, message) do
    GRPC.Server.send_reply(stream, message)
    :ok
  rescue
    error ->
      {:error, error}
  end
end
