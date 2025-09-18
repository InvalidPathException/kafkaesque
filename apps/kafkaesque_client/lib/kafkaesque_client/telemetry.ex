defmodule KafkaesqueClient.Telemetry do
  @moduledoc """
  Telemetry integration for Kafkaesque client metrics.

  Emits telemetry events for:
  - Producer operations (send latency, batch size, errors)
  - Consumer operations (poll latency, lag, commit success)
  - Connection events (connects, disconnects, errors)
  - Admin operations (topic creation, deletion)
  """

  require Logger

  @producer_events [
    [:kafkaesque_client, :producer, :send],
    [:kafkaesque_client, :producer, :batch],
    [:kafkaesque_client, :producer, :error]
  ]

  @consumer_events [
    [:kafkaesque_client, :consumer, :poll],
    [:kafkaesque_client, :consumer, :commit],
    [:kafkaesque_client, :consumer, :lag],
    [:kafkaesque_client, :consumer, :error]
  ]

  @connection_events [
    [:kafkaesque_client, :connection, :connect],
    [:kafkaesque_client, :connection, :disconnect],
    [:kafkaesque_client, :connection, :error]
  ]

  @admin_events [
    [:kafkaesque_client, :admin, :operation],
    [:kafkaesque_client, :admin, :error]
  ]

  @doc """
  Returns a list of all telemetry events emitted by the client.
  """
  def events do
    @producer_events ++ @consumer_events ++ @connection_events ++ @admin_events
  end

  @doc """
  Attaches default handlers for logging telemetry events.
  """
  def attach_default_handlers do
    events()
    |> Enum.each(fn event ->
      handler_id = "#{__MODULE__}-#{Enum.join(event, "-")}"

      :telemetry.attach(
        handler_id,
        event,
        &handle_event/4,
        nil
      )
    end)

    :ok
  end

  @doc """
  Records producer send latency.
  """
  def record_produce_latency(duration_ns) do
    :telemetry.execute(
      [:kafkaesque_client, :producer, :send],
      %{duration: duration_ns},
      %{unit: :nanosecond}
    )
  end

  @doc """
  Records a producer batch being sent.
  """
  def record_producer_batch(topic, record_count, batch_size_bytes) do
    :telemetry.execute(
      [:kafkaesque_client, :producer, :batch],
      %{
        record_count: record_count,
        batch_size_bytes: batch_size_bytes
      },
      %{topic: topic}
    )
  end

  @doc """
  Records a producer error.
  """
  def record_producer_error(topic, reason) do
    :telemetry.execute(
      [:kafkaesque_client, :producer, :error],
      %{count: 1},
      %{topic: topic, reason: inspect(reason)}
    )
  end

  @doc """
  Records consumer poll metrics.
  """
  def record_consumer_poll(group_id, record_count, duration_ms) do
    :telemetry.execute(
      [:kafkaesque_client, :consumer, :poll],
      %{
        record_count: record_count,
        duration: duration_ms
      },
      %{group_id: group_id, unit: :millisecond}
    )
  end

  @doc """
  Records consumer commit metrics.
  """
  def record_consumer_commit(group_id, offset_count, success?) do
    :telemetry.execute(
      [:kafkaesque_client, :consumer, :commit],
      %{
        offset_count: offset_count,
        success: if(success?, do: 1, else: 0)
      },
      %{group_id: group_id}
    )
  end

  @doc """
  Records consumer lag.
  """
  def record_consumer_lag(group_id, topic, partition, lag) do
    :telemetry.execute(
      [:kafkaesque_client, :consumer, :lag],
      %{lag: lag},
      %{
        group_id: group_id,
        topic: topic,
        partition: partition
      }
    )
  end

  @doc """
  Records a consumer error.
  """
  def record_consumer_error(group_id, reason) do
    :telemetry.execute(
      [:kafkaesque_client, :consumer, :error],
      %{count: 1},
      %{group_id: group_id, reason: inspect(reason)}
    )
  end

  @doc """
  Records a connection being established.
  """
  def record_connection_connect(server) do
    :telemetry.execute(
      [:kafkaesque_client, :connection, :connect],
      %{count: 1},
      %{server: server}
    )
  end

  @doc """
  Records a connection being disconnected.
  """
  def record_connection_disconnect(server, reason) do
    :telemetry.execute(
      [:kafkaesque_client, :connection, :disconnect],
      %{count: 1},
      %{server: server, reason: inspect(reason)}
    )
  end

  @doc """
  Records a connection error.
  """
  def record_connection_error(server, reason) do
    :telemetry.execute(
      [:kafkaesque_client, :connection, :error],
      %{count: 1},
      %{server: server, reason: inspect(reason)}
    )
  end

  @doc """
  Records an admin operation.
  """
  def record_admin_operation(operation, duration_ms, success?) do
    :telemetry.execute(
      [:kafkaesque_client, :admin, :operation],
      %{
        duration: duration_ms,
        success: if(success?, do: 1, else: 0)
      },
      %{operation: operation, unit: :millisecond}
    )
  end

  @doc """
  Records an admin error.
  """
  def record_admin_error(operation, reason) do
    :telemetry.execute(
      [:kafkaesque_client, :admin, :error],
      %{count: 1},
      %{operation: operation, reason: inspect(reason)}
    )
  end

  # Private handler for default logging

  defp handle_event(event, measurements, metadata, _config) do
    event_name = Enum.join(event, ".")

    Logger.debug(
      "Telemetry event: #{event_name}, measurements: #{inspect(measurements)}, metadata: #{inspect(metadata)}"
    )
  end

  @doc """
  Helper to create a Telemetry.Metrics definition for Prometheus/Grafana.

  ## Example

      import Telemetry.Metrics

      def metrics do
        KafkaesqueClient.Telemetry.metrics_definitions()
      end
  """
  def metrics_definitions do
    [
      # Producer metrics
      summary("kafkaesque_client.producer.send.duration",
        unit: {:nanosecond, :millisecond},
        description: "Producer send latency"
      ),
      counter("kafkaesque_client.producer.batch.record_count",
        description: "Number of records sent"
      ),
      summary("kafkaesque_client.producer.batch.batch_size_bytes",
        description: "Producer batch size in bytes"
      ),
      counter("kafkaesque_client.producer.error.count",
        description: "Producer error count",
        tags: [:topic, :reason]
      ),

      # Consumer metrics
      summary("kafkaesque_client.consumer.poll.duration",
        unit: {:millisecond, :millisecond},
        description: "Consumer poll duration"
      ),
      counter("kafkaesque_client.consumer.poll.record_count",
        description: "Records consumed",
        tags: [:group_id]
      ),
      counter("kafkaesque_client.consumer.commit.success",
        description: "Successful commits",
        tags: [:group_id]
      ),
      last_value("kafkaesque_client.consumer.lag.lag",
        description: "Consumer lag",
        tags: [:group_id, :topic, :partition]
      ),
      counter("kafkaesque_client.consumer.error.count",
        description: "Consumer error count",
        tags: [:group_id, :reason]
      ),

      # Connection metrics
      counter("kafkaesque_client.connection.connect.count",
        description: "Connection established",
        tags: [:server]
      ),
      counter("kafkaesque_client.connection.disconnect.count",
        description: "Connection disconnected",
        tags: [:server, :reason]
      ),
      counter("kafkaesque_client.connection.error.count",
        description: "Connection errors",
        tags: [:server, :reason]
      ),

      # Admin metrics
      summary("kafkaesque_client.admin.operation.duration",
        unit: {:millisecond, :millisecond},
        description: "Admin operation duration",
        tags: [:operation]
      ),
      counter("kafkaesque_client.admin.operation.success",
        description: "Successful admin operations",
        tags: [:operation]
      ),
      counter("kafkaesque_client.admin.error.count",
        description: "Admin operation errors",
        tags: [:operation, :reason]
      )
    ]
  end

  # Helper functions for metric definitions
  defp summary(name, opts) do
    struct(Telemetry.Metrics.Summary, [name: name] ++ opts)
  end

  defp counter(name, opts) do
    struct(Telemetry.Metrics.Counter, [name: name] ++ opts)
  end

  defp last_value(name, opts) do
    struct(Telemetry.Metrics.LastValue, [name: name] ++ opts)
  end
end
