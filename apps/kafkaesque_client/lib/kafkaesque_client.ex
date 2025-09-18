defmodule KafkaesqueClient do
  @moduledoc """
  Elixir client SDK for Kafkaesque - A Kafka-compatible distributed log service.

  This module provides factory methods for creating producers, consumers, and admin clients
  that mirror Apache Kafka's client API design.

  ## Quick Start

      # Create a producer
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50051"],
        acks: :all
      )

      # Send a message
      record = KafkaesqueClient.producer_record("events", "key-1", "value-1")
      {:ok, metadata} = KafkaesqueClient.Producer.send_sync(producer, record)

      # Create a consumer
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50051"],
        group_id: "my-group",
        auto_offset_reset: :earliest
      )

      # Subscribe and poll
      :ok = KafkaesqueClient.Consumer.subscribe(consumer, ["events"])
      records = KafkaesqueClient.Consumer.poll(consumer)

      # Create an admin client
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50051"]
      )

      # Create a topic
      {:ok, topic} = KafkaesqueClient.Admin.create_topic(admin, "new-topic", partitions: 3)
  """

  alias KafkaesqueClient.{Admin, Consumer, Producer}
  alias KafkaesqueClient.Connection.Supervisor, as: ConnectionSupervisor
  alias KafkaesqueClient.Record.{OffsetAndMetadata, ProducerRecord, TopicPartition}

  @doc """
  Starts the client application and its supervision tree.

  This is typically called automatically when the client is added as a dependency,
  but can be called manually if needed.
  """
  def start(_type \\ :normal, _args \\ []) do
    children = [
      ConnectionSupervisor
    ]

    opts = [strategy: :one_for_one, name: KafkaesqueClient.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Creates a new producer with the given configuration.

  ## Options

  - `:bootstrap_servers` - List of Kafkaesque servers (required)
  - `:acks` - `:none`, `:leader`, or `:all` (default: `:leader`)
  - `:batch_size` - Maximum batch size in bytes (default: 16384)
  - `:linger_ms` - Time to wait before sending incomplete batches (default: 100)
  - `:compression_type` - Compression type (default: `:none`, future: `:gzip`, `:snappy`)
  - `:max_retries` - Maximum number of retries (default: 3)
  - `:retry_backoff_ms` - Backoff between retries (default: 100)

  ## Examples

      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50051"],
        acks: :all,
        batch_size: 32768,
        linger_ms: 200
      )
  """
  def create_producer(opts) when is_list(opts) do
    config = Map.new(opts)
    Producer.start_link(config)
  end

  @doc """
  Creates a new consumer with the given configuration.

  ## Options

  - `:bootstrap_servers` - List of Kafkaesque servers (required)
  - `:group_id` - Consumer group ID (required)
  - `:auto_offset_reset` - `:earliest` or `:latest` (default: `:latest`)
  - `:enable_auto_commit` - Enable auto-commit (default: `true`)
  - `:auto_commit_interval_ms` - Auto-commit interval (default: 5000)
  - `:session_timeout_ms` - Session timeout (default: 30000)
  - `:max_poll_records` - Maximum records per poll (default: 500)

  ## Examples

      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50051"],
        group_id: "my-service",
        auto_offset_reset: :earliest,
        enable_auto_commit: true
      )
  """
  def create_consumer(opts) when is_list(opts) do
    config = Map.new(opts)
    Consumer.start_link(config)
  end

  @doc """
  Creates a new admin client with the given configuration.

  ## Options

  - `:bootstrap_servers` - List of Kafkaesque servers (required)
  - `:request_timeout_ms` - Request timeout (default: 30000)

  ## Examples

      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50051"]
      )
  """
  def create_admin(opts) when is_list(opts) do
    config = Map.new(opts)
    Admin.start_link(config)
  end

  @doc """
  Creates a ProducerRecord for sending to Kafkaesque.

  ## Examples

      # Simple record with topic and value
      record = KafkaesqueClient.producer_record("events", "my-value")

      # Record with key
      record = KafkaesqueClient.producer_record("events", "key-1", "value-1")

      # Record with headers
      record = KafkaesqueClient.producer_record("events", "key-1", "value-1",
        headers: %{"trace_id" => "abc-123"}
      )

      # Full record specification
      record = %ProducerRecord{
        topic: "events",
        partition: 0,
        key: "user-123",
        value: Jason.encode!(%{action: "login"}),
        headers: %{"trace_id" => "abc-123"},
        timestamp: System.system_time(:millisecond)
      }
  """
  def producer_record(topic, value) when is_binary(topic) and is_binary(value) do
    ProducerRecord.new(topic, value)
  end

  def producer_record(topic, key, value) when is_binary(topic) do
    ProducerRecord.new(topic, key, value)
  end

  def producer_record(topic, key, value, opts) when is_binary(topic) and is_list(opts) do
    base_record = ProducerRecord.new(topic, key, value)
    struct(base_record, opts)
  end

  @doc """
  Creates a TopicPartition reference.

  ## Examples

      tp = KafkaesqueClient.topic_partition("events", 0)
  """
  def topic_partition(topic, partition \\ 0) do
    TopicPartition.new(topic, partition)
  end

  @doc """
  Creates an OffsetAndMetadata for committing.

  ## Examples

      om = KafkaesqueClient.offset_and_metadata(100)
      om = KafkaesqueClient.offset_and_metadata(100, "checkpoint-1")
  """
  def offset_and_metadata(offset, metadata \\ nil) do
    OffsetAndMetadata.new(offset, metadata)
  end

  @doc """
  Attaches default telemetry handlers for logging metrics.

  This sets up handlers that log all client telemetry events.
  Call this once at application startup if you want default logging.

  ## Examples

      KafkaesqueClient.attach_telemetry_handlers()
  """
  def attach_telemetry_handlers do
    KafkaesqueClient.Telemetry.attach_default_handlers()
  end

  @doc """
  Returns telemetry event definitions for monitoring.

  Use this with Telemetry.Metrics for Prometheus/Grafana integration.

  ## Examples

      def metrics do
        KafkaesqueClient.telemetry_metrics()
      end
  """
  def telemetry_metrics do
    KafkaesqueClient.Telemetry.metrics_definitions()
  end
end
