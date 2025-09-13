defmodule Kafkaesque.Telemetry do
  @moduledoc """
  Telemetry event definitions and metrics aggregation for Kafkaesque.
  Uses Flow for window-based aggregation of metrics.
  """

  use GenServer
  require Logger

  @metrics_window_ms 5_000
  @percentiles [50, 95, 99]

  # Telemetry event names
  @message_produced [:kafkaesque, :message, :produced]
  @message_consumed [:kafkaesque, :message, :consumed]
  @topic_created [:kafkaesque, :topic, :created]
  @topic_deleted [:kafkaesque, :topic, :deleted]
  @consumer_joined [:kafkaesque, :consumer, :joined]
  @consumer_left [:kafkaesque, :consumer, :left]
  @rebalance_started [:kafkaesque, :rebalance, :started]
  @rebalance_completed [:kafkaesque, :rebalance, :completed]
  @offset_committed [:kafkaesque, :offset, :committed]
  @lag_measured [:kafkaesque, :lag, :measured]
  @storage_write [:kafkaesque, :storage, :write]
  @storage_read [:kafkaesque, :storage, :read]

  defmodule State do
    @moduledoc false
    defstruct [
      :flow_pid,
      :metrics_table,
      :subscribers,
      :aggregated_metrics
    ]
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Emit a message produced event.
  """
  def message_produced(metadata) do
    measurements = %{
      count: 1,
      bytes: metadata[:bytes] || 0,
      timestamp: System.system_time(:millisecond)
    }

    :telemetry.execute(@message_produced, measurements, metadata)
  end

  @doc """
  Emit a message consumed event.
  """
  def message_consumed(metadata) do
    measurements = %{
      count: 1,
      bytes: metadata[:bytes] || 0,
      latency_ms: metadata[:latency_ms] || 0,
      timestamp: System.system_time(:millisecond)
    }

    :telemetry.execute(@message_consumed, measurements, metadata)
  end

  @doc """
  Emit a topic created event.
  """
  def topic_created(topic_name, partitions) do
    measurements = %{
      partitions: partitions,
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      topic: topic_name
    }

    :telemetry.execute(@topic_created, measurements, metadata)
  end

  @doc """
  Emit a consumer group lag measurement.
  """
  def lag_measured(group_id, topic, partition, lag) do
    measurements = %{
      lag: lag,
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      group_id: group_id,
      topic: topic,
      partition: partition
    }

    :telemetry.execute(@lag_measured, measurements, metadata)
  end

  @doc """
  Emit a topic deleted event.
  """
  def topic_deleted(topic_name) do
    measurements = %{
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      topic: topic_name
    }

    :telemetry.execute(@topic_deleted, measurements, metadata)
  end

  @doc """
  Emit a consumer joined event.
  """
  def consumer_joined(group_id, consumer_id) do
    measurements = %{
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      group_id: group_id,
      consumer_id: consumer_id
    }

    :telemetry.execute(@consumer_joined, measurements, metadata)
  end

  @doc """
  Emit a consumer left event.
  """
  def consumer_left(group_id, consumer_id) do
    measurements = %{
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      group_id: group_id,
      consumer_id: consumer_id
    }

    :telemetry.execute(@consumer_left, measurements, metadata)
  end

  @doc """
  Emit a rebalance started event.
  """
  def rebalance_started(group_id) do
    measurements = %{
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      group_id: group_id
    }

    :telemetry.execute(@rebalance_started, measurements, metadata)
  end

  @doc """
  Emit a rebalance completed event.
  """
  def rebalance_completed(group_id, duration_ms) do
    measurements = %{
      duration_ms: duration_ms,
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      group_id: group_id
    }

    :telemetry.execute(@rebalance_completed, measurements, metadata)
  end

  @doc """
  Emit an offset committed event.
  """
  def offset_committed(group_id, topic, partition, offset) do
    measurements = %{
      offset: offset,
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      group_id: group_id,
      topic: topic,
      partition: partition
    }

    :telemetry.execute(@offset_committed, measurements, metadata)
  end

  @doc """
  Emit a storage write event.
  """
  def storage_write(topic, partition, bytes, duration_ms) do
    measurements = %{
      bytes: bytes,
      duration_ms: duration_ms,
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      topic: topic,
      partition: partition
    }

    :telemetry.execute(@storage_write, measurements, metadata)
  end

  @doc """
  Emit a storage read event.
  """
  def storage_read(topic, partition, bytes, duration_ms) do
    measurements = %{
      bytes: bytes,
      duration_ms: duration_ms,
      timestamp: System.system_time(:millisecond)
    }

    metadata = %{
      topic: topic,
      partition: partition
    }

    :telemetry.execute(@storage_read, measurements, metadata)
  end

  @doc """
  Subscribe to aggregated metrics updates.
  """
  def subscribe do
    Phoenix.PubSub.subscribe(Kafkaesque.PubSub, "metrics:aggregated")
  end

  @doc """
  Get current aggregated metrics.
  """
  def get_metrics do
    GenServer.call(__MODULE__, :get_metrics)
  end

  @impl true
  def init(_opts) do
    # Create ETS table for storing raw metrics
    table = :ets.new(:telemetry_metrics, [:set, :public])

    # Attach telemetry handlers
    attach_handlers()

    # Start Flow for metrics aggregation
    flow_pid = start_metrics_flow()

    # Schedule periodic aggregation
    schedule_aggregation()

    state = %State{
      flow_pid: flow_pid,
      metrics_table: table,
      subscribers: [],
      aggregated_metrics: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:get_metrics, _from, state) do
    {:reply, state.aggregated_metrics, state}
  end

  @impl true
  def handle_info(:aggregate_metrics, state) do
    # Trigger Flow window completion
    # In a real implementation, we'd use Flow's windowing
    metrics = calculate_aggregated_metrics(state.metrics_table)

    # Broadcast to subscribers
    Phoenix.PubSub.broadcast(
      Kafkaesque.PubSub,
      "metrics:aggregated",
      {:metrics_updated, metrics}
    )

    # Schedule next aggregation
    schedule_aggregation()

    {:noreply, %{state | aggregated_metrics: metrics}}
  end

  defp attach_handlers do
    handlers = [
      {@message_produced, &handle_message_produced/4},
      {@message_consumed, &handle_message_consumed/4},
      {@topic_created, &handle_topic_created/4},
      {@topic_deleted, &handle_topic_deleted/4},
      {@consumer_joined, &handle_consumer_joined/4},
      {@consumer_left, &handle_consumer_left/4},
      {@rebalance_started, &handle_rebalance_started/4},
      {@rebalance_completed, &handle_rebalance_completed/4},
      {@offset_committed, &handle_offset_committed/4},
      {@lag_measured, &handle_lag_measured/4},
      {@storage_write, &handle_storage_write/4},
      {@storage_read, &handle_storage_read/4}
    ]

    Enum.each(handlers, fn {event, handler} ->
      :telemetry.attach(
        "#{inspect(event)}-handler",
        event,
        handler,
        nil
      )
    end)
  end

  defp handle_message_produced(_event, measurements, metadata, _config) do
    # Store in ETS for aggregation
    if :ets.whereis(:telemetry_metrics) != :undefined do
      key = {:messages_produced, metadata[:topic], metadata[:partition]}
      timestamp = measurements[:timestamp]

      :ets.insert(:telemetry_metrics, {
        {key, timestamp},
        measurements[:count],
        measurements[:bytes]
      })
    end
  end

  defp handle_message_consumed(_event, measurements, metadata, _config) do
    if :ets.whereis(:telemetry_metrics) != :undefined do
      key = {:messages_consumed, metadata[:topic], metadata[:partition]}
      timestamp = measurements[:timestamp]

      :ets.insert(:telemetry_metrics, {
        {key, timestamp},
        measurements[:count],
        measurements[:bytes],
        measurements[:latency_ms]
      })
    end
  end

  defp handle_topic_created(_event, measurements, metadata, _config) do
    Logger.info("Topic created: #{metadata[:topic]} with #{measurements[:partitions]} partitions")
  end

  defp handle_topic_deleted(_event, _measurements, metadata, _config) do
    Logger.info("Topic deleted: #{metadata[:topic]}")
  end

  defp handle_consumer_joined(_event, _measurements, metadata, _config) do
    Logger.info("Consumer joined: #{metadata[:consumer_id]} in group #{metadata[:group_id]}")
  end

  defp handle_consumer_left(_event, _measurements, metadata, _config) do
    Logger.info("Consumer left: #{metadata[:consumer_id]} from group #{metadata[:group_id]}")
  end

  defp handle_rebalance_started(_event, _measurements, metadata, _config) do
    Logger.info("Rebalance started for group: #{metadata[:group_id]}")
  end

  defp handle_rebalance_completed(_event, measurements, metadata, _config) do
    Logger.info(
      "Rebalance completed for group: #{metadata[:group_id]} in #{measurements[:duration_ms]}ms"
    )
  end

  defp handle_offset_committed(_event, measurements, metadata, _config) do
    if :ets.whereis(:telemetry_metrics) != :undefined do
      key = {:offset_committed, metadata[:group_id], metadata[:topic], metadata[:partition]}
      timestamp = measurements[:timestamp]

      :ets.insert(:telemetry_metrics, {
        {key, timestamp},
        measurements[:offset]
      })
    end
  end

  defp handle_lag_measured(_event, measurements, metadata, _config) do
    if :ets.whereis(:telemetry_metrics) != :undefined do
      key = {:lag, metadata[:group_id], metadata[:topic], metadata[:partition]}
      timestamp = measurements[:timestamp]

      :ets.insert(:telemetry_metrics, {
        {key, timestamp},
        measurements[:lag]
      })
    end
  end

  defp handle_storage_write(_event, measurements, metadata, _config) do
    # Check if ETS table exists before inserting
    if :ets.whereis(:telemetry_metrics) != :undefined do
      key = {:storage_write, metadata[:topic], metadata[:partition]}
      timestamp = measurements[:timestamp]

      :ets.insert(:telemetry_metrics, {
        {key, timestamp},
        measurements[:bytes],
        measurements[:duration_ms]
      })
    end
  end

  defp handle_storage_read(_event, measurements, metadata, _config) do
    # Check if ETS table exists before inserting
    if :ets.whereis(:telemetry_metrics) != :undefined do
      key = {:storage_read, metadata[:topic], metadata[:partition]}
      timestamp = measurements[:timestamp]

      :ets.insert(:telemetry_metrics, {
        {key, timestamp},
        measurements[:bytes],
        measurements[:duration_ms]
      })
    end
  end

  defp start_metrics_flow do
    # Mock Flow pipeline for metrics aggregation
    # In a real implementation, this would use Flow.from_enumerable
    # with windows and aggregation
    spawn_link(fn ->
      Process.sleep(:infinity)
    end)
  end

  defp calculate_aggregated_metrics(table) do
    now = System.system_time(:millisecond)
    window_start = now - @metrics_window_ms

    # Get all metrics within the window
    metrics =
      :ets.tab2list(table)
      |> Enum.filter(fn {{_key, timestamp}, _data} ->
        timestamp >= window_start
      end)

    # Clean old metrics
    :ets.select_delete(table, [
      {{{:_, :"$1"}, :_}, [{:<, :"$1", window_start}], [true]}
    ])

    # Aggregate metrics
    %{
      messages_per_sec: calculate_rate(metrics, :messages_produced),
      bytes_per_sec: calculate_bytes_rate(metrics, :messages_produced),
      consume_rate: calculate_rate(metrics, :messages_consumed),
      latency_percentiles: calculate_latency_percentiles(metrics),
      total_lag: calculate_total_lag(metrics),
      storage_throughput: calculate_storage_throughput(metrics),
      timestamp: now
    }
  end

  defp calculate_rate(metrics, type) do
    count =
      metrics
      |> Enum.filter(fn {{key, _}, _} ->
        match?({^type, _, _}, key)
      end)
      |> Enum.map(fn {_, count, _} -> count end)
      |> Enum.sum()

    # Convert to per-second rate
    count * 1000 / @metrics_window_ms
  end

  defp calculate_bytes_rate(metrics, type) do
    bytes =
      metrics
      |> Enum.filter(fn {{key, _}, _} ->
        match?({^type, _, _}, key)
      end)
      |> Enum.map(fn {_, _, bytes} -> bytes end)
      |> Enum.sum()

    # Convert to per-second rate
    bytes * 1000 / @metrics_window_ms
  end

  defp calculate_latency_percentiles(metrics) do
    latencies =
      metrics
      |> Enum.filter(fn {{key, _}, _} ->
        match?({:messages_consumed, _, _}, key)
      end)
      |> Enum.map(fn {_, _, _, latency} -> latency end)
      |> Enum.sort()

    if length(latencies) > 0 do
      Enum.map(@percentiles, fn p ->
        index = round(length(latencies) * p / 100)
        {p, Enum.at(latencies, min(index, length(latencies) - 1))}
      end)
      |> Enum.into(%{})
    else
      %{50 => 0, 95 => 0, 99 => 0}
    end
  end

  defp calculate_total_lag(metrics) do
    metrics
    |> Enum.filter(fn {{key, _}, _} ->
      match?({:lag, _, _, _}, key)
    end)
    |> Enum.map(fn {_, lag} -> lag end)
    |> Enum.sum()
  end

  defp calculate_storage_throughput(metrics) do
    storage_metrics =
      metrics
      |> Enum.filter(fn {{key, _}, _} ->
        match?({:storage_write, _, _}, key)
      end)

    total_bytes =
      storage_metrics
      |> Enum.map(fn {_, bytes, _} -> bytes end)
      |> Enum.sum()

    total_duration =
      storage_metrics
      |> Enum.map(fn {_, _, duration} -> duration end)
      |> Enum.sum()

    if total_duration > 0 do
      # bytes per second
      total_bytes * 1000 / total_duration
    else
      0
    end
  end

  defp schedule_aggregation do
    Process.send_after(self(), :aggregate_metrics, @metrics_window_ms)
  end
end
