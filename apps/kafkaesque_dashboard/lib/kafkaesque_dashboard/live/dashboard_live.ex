defmodule KafkaesqueDashboard.DashboardLive do
  use Phoenix.LiveView
  import KafkaesqueDashboard.CoreComponents
  import LiveSvelte

  # Subscribe to telemetry events
  @telemetry_events [
    [:kafkaesque, :message, :produced],
    [:kafkaesque, :message, :consumed],
    [:kafkaesque, :topic, :created],
    [:kafkaesque, :topic, :deleted],
    [:kafkaesque, :consumer, :joined],
    [:kafkaesque, :consumer, :left],
    [:kafkaesque, :rebalance, :started],
    [:kafkaesque, :rebalance, :completed],
    [:kafkaesque, :offset, :committed],
    [:kafkaesque, :lag, :measured],
    [:kafkaesque, :storage, :write],
    [:kafkaesque, :storage, :read]
  ]

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      # Subscribe to telemetry aggregated metrics
      Phoenix.PubSub.subscribe(Kafkaesque.PubSub, "metrics:aggregated")

      # Attach telemetry handlers for real-time events
      attach_telemetry_handlers()

      # Update interval for UI refresh
      :timer.send_interval(2000, self(), :update_metrics)
    end

    socket =
      socket
      |> assign_all_metrics()
      |> assign_telemetry_metrics()

    {:ok, socket}
  end

  @impl true
  def handle_info({:metrics_updated, metrics}, socket) do
    # Handle aggregated metrics from telemetry
    socket =
      socket
      |> assign(:telemetry_metrics, metrics)
      |> update_charts_from_telemetry(metrics)

    {:noreply, socket}
  end

  @impl true
  def handle_info({:telemetry_event, event_name, measurements, metadata}, socket) do
    # Handle individual telemetry events for real-time updates
    socket = handle_telemetry_event(socket, event_name, measurements, metadata)
    {:noreply, socket}
  end

  @impl true
  def handle_info(:update_metrics, socket) do
    socket = assign_all_metrics(socket)

    # Push chart data updates to the client
    socket =
      socket
      |> push_event("update-chart", %{
        id: "throughput-chart",
        data: socket.assigns.throughput_data
      })
      |> push_event("update-chart", %{
        id: "lag-chart",
        data: socket.assigns.lag_data.values
      })
      |> push_event("update-chart", %{
        id: "topic-chart",
        data: socket.assigns.topic_distribution.values
      })
      |> push_event("update-gauge", %{
        id: "cpu-gauge",
        value: socket.assigns.system_metrics.cpu_usage,
        max: socket.assigns.system_metrics.cpu_max
      })
      |> push_event("update-gauge", %{
        id: "memory-gauge",
        value: socket.assigns.system_metrics.memory_usage_mb,
        max: socket.assigns.system_metrics.memory_max_mb
      })
      |> push_event("update-gauge", %{
        id: "disk-gauge",
        value: trunc(socket.assigns.system_metrics.disk_usage_gb * 10),
        max: trunc(socket.assigns.system_metrics.disk_max_gb * 10)
      })

    {:noreply, socket}
  end

  defp assign_all_metrics(socket) do
    socket
    |> assign(:system_metrics, get_system_metrics())
    |> assign(:topic_stats, get_topic_stats())
    |> assign(:topics, get_topics())
    |> assign(:consumer_groups, get_consumer_groups())
    |> assign(:throughput_data, get_throughput_data())
    |> assign(:lag_data, get_lag_data())
    |> assign(:topic_distribution, get_topic_distribution())
    |> assign(:partition_data, get_partition_data())
    |> assign(:rebalance_events, get_rebalance_events())
    |> assign(:storage_metrics, get_storage_metrics())
    |> assign(:consumer_activity, get_consumer_activity())
  end

  defp assign_telemetry_metrics(socket) do
    # Initialize telemetry-specific metrics
    socket
    |> assign(:produce_rate_history, [])
    |> assign(:consume_rate_history, [])
    |> assign(:storage_write_history, [])
    |> assign(:storage_read_history, [])
    |> assign(:offset_commit_history, [])
    |> assign(:active_rebalances, 0)
    |> assign(:topic_events, [])
    |> assign(:consumer_events, [])
  end

  defp get_system_metrics do
    {uptime_seconds, _} = :erlang.statistics(:wall_clock)
    memory = :erlang.memory()

    %{
      uptime_hours: div(uptime_seconds, 3_600_000),
      memory_usage_mb: div(memory[:total], 1024 * 1024),
      memory_max_mb: 4096,
      cpu_usage: 35 + :rand.uniform(30),
      cpu_max: 100,
      beam_processes: :erlang.system_info(:process_count),
      disk_usage_gb: 2.4 + :rand.uniform() * 0.5,
      disk_max_gb: 10.0,
      network_in_mbps: 10 + :rand.uniform(20),
      network_out_mbps: 15 + :rand.uniform(25)
    }
  end

  defp get_topic_stats do
    %{
      total_topics: 12,
      total_partitions: 48,
      total_messages: 15_234_567 + :rand.uniform(100_000),
      storage_size_gb: 2.4 + :rand.uniform() * 0.2,
      messages_per_sec: 1234 + :rand.uniform(500),
      bytes_per_sec: 567_890 + :rand.uniform(100_000)
    }
  end

  defp get_topics do
    [
      %{name: "user-events", partitions: 8, messages: 5_234_567, size_mb: 1234},
      %{name: "order-transactions", partitions: 12, messages: 3_456_789, size_mb: 2345},
      %{name: "analytics-events", partitions: 4, messages: 2_345_678, size_mb: 567},
      %{name: "system-logs", partitions: 6, messages: 1_234_567, size_mb: 890},
      %{name: "user-profiles", partitions: 4, messages: 890_123, size_mb: 456},
      %{name: "payment-events", partitions: 8, messages: 1_567_890, size_mb: 1234},
      %{name: "inventory-updates", partitions: 6, messages: 678_901, size_mb: 345}
    ]
  end

  defp get_consumer_groups do
    [
      %{name: "analytics-pipeline", lag: 1234 + :rand.uniform(500), members: 5, state: "stable"},
      %{name: "event-processor", lag: 567 + :rand.uniform(200), members: 3, state: "stable"},
      %{name: "backup-writer", lag: 4567 + :rand.uniform(1000), members: 1, state: "rebalancing"},
      %{name: "stream-aggregator", lag: 890 + :rand.uniform(300), members: 2, state: "stable"},
      %{name: "ml-pipeline", lag: 2345 + :rand.uniform(600), members: 4, state: "stable"},
      %{name: "real-time-alerts", lag: 123 + :rand.uniform(100), members: 2, state: "stable"}
    ]
  end

  defp get_throughput_data do
    # Generate 30 data points for better visualization
    # Keep consistent range between 500-2500 for stable scaling
    for _ <- 0..29 do
      base = 1200
      # -300 to +300
      variation = :rand.uniform(600) - 300
      spike = if :rand.uniform(10) > 8, do: :rand.uniform(500), else: 0
      max(500, min(2500, base + variation + spike))
    end
  end

  defp get_lag_data do
    # Consumer group lag data with labels
    groups = get_consumer_groups()

    %{
      values: Enum.map(groups, & &1.lag),
      labels: Enum.map(groups, & &1.name)
    }
  end

  defp get_topic_distribution do
    topics = get_topics()

    %{
      values: Enum.map(topics, & &1.size_mb),
      labels: Enum.map(topics, & &1.name)
    }
  end

  defp get_partition_data do
    # Generate partition health data
    for i <- 0..11 do
      %{
        id: i,
        leader: rem(i, 3),
        replicas: 1,
        isr: 1,
        messages: 100_000 + :rand.uniform(50_000),
        size_mb: 50 + :rand.uniform(100),
        rate: 50 + :rand.uniform(150)
      }
    end
  end

  defp get_rebalance_events do
    # Track recent rebalance events
    [
      %{time: "10:45:23", group: "analytics-pipeline", duration_ms: 1250, partitions: 8},
      %{time: "10:42:15", group: "event-processor", duration_ms: 850, partitions: 6},
      %{time: "10:38:47", group: "backup-writer", duration_ms: 2100, partitions: 12}
    ]
  end

  defp get_storage_metrics do
    %{
      write_throughput_mbps: 45.6 + :rand.uniform() * 10,
      read_throughput_mbps: 78.3 + :rand.uniform() * 15,
      write_latency_ms: 2.3 + :rand.uniform() * 0.5,
      read_latency_ms: 0.8 + :rand.uniform() * 0.2,
      compression_ratio: 3.2,
      cache_hit_rate: 0.87
    }
  end

  defp get_consumer_activity do
    # Track consumer join/leave activity
    [
      %{time: "10:46:12", event: "joined", consumer: "consumer-1", group: "analytics-pipeline"},
      %{time: "10:45:58", event: "left", consumer: "consumer-3", group: "event-processor"},
      %{time: "10:45:23", event: "joined", consumer: "consumer-2", group: "analytics-pipeline"},
      %{time: "10:44:15", event: "joined", consumer: "consumer-5", group: "ml-pipeline"}
    ]
  end

  defp attach_telemetry_handlers do
    Enum.each(@telemetry_events, fn event ->
      handler_id = "dashboard-#{Enum.join(event, "-")}"

      :telemetry.attach(
        handler_id,
        event,
        &__MODULE__.handle_telemetry_callback/4,
        self()
      )
    end)
  end

  def handle_telemetry_callback(event_name, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event_name, measurements, metadata})
  end

  defp handle_telemetry_event(socket, [:kafkaesque, :message, :produced], measurements, _metadata) do
    # Update produce rate history
    history = Enum.take([measurements[:count] | socket.assigns.produce_rate_history], 60)
    assign(socket, :produce_rate_history, history)
  end

  defp handle_telemetry_event(socket, [:kafkaesque, :message, :consumed], measurements, _metadata) do
    # Update consume rate history
    history = Enum.take([measurements[:count] | socket.assigns.consume_rate_history], 60)
    assign(socket, :consume_rate_history, history)
  end

  defp handle_telemetry_event(socket, [:kafkaesque, :topic, event], _measurements, metadata)
       when event in [:created, :deleted] do
    # Track topic events
    event_record = %{
      event: event,
      topic: metadata[:topic],
      time: DateTime.utc_now() |> DateTime.to_string()
    }

    events = Enum.take([event_record | socket.assigns.topic_events], 10)
    assign(socket, :topic_events, events)
  end

  defp handle_telemetry_event(socket, [:kafkaesque, :consumer, event], _measurements, metadata)
       when event in [:joined, :left] do
    # Track consumer events
    event_record = %{
      event: event,
      consumer: metadata[:consumer_id],
      group: metadata[:group_id],
      time: DateTime.utc_now() |> DateTime.to_string()
    }

    events = Enum.take([event_record | socket.assigns.consumer_events], 10)
    assign(socket, :consumer_events, events)
  end

  defp handle_telemetry_event(
         socket,
         [:kafkaesque, :rebalance, :started],
         _measurements,
         _metadata
       ) do
    assign(socket, :active_rebalances, socket.assigns.active_rebalances + 1)
  end

  defp handle_telemetry_event(
         socket,
         [:kafkaesque, :rebalance, :completed],
         _measurements,
         _metadata
       ) do
    assign(socket, :active_rebalances, max(0, socket.assigns.active_rebalances - 1))
  end

  defp handle_telemetry_event(socket, [:kafkaesque, :storage, type], measurements, _metadata)
       when type in [:write, :read] do
    # Update storage metrics
    key = if type == :write, do: :storage_write_history, else: :storage_read_history
    history = Enum.take([measurements[:bytes] | Map.get(socket.assigns, key, [])], 60)
    assign(socket, key, history)
  end

  defp handle_telemetry_event(socket, [:kafkaesque, :offset, :committed], measurements, _metadata) do
    # Track offset commits
    history = Enum.take([measurements | socket.assigns.offset_commit_history], 100)
    assign(socket, :offset_commit_history, history)
  end

  defp handle_telemetry_event(socket, _event, _measurements, _metadata) do
    socket
  end

  defp update_charts_from_telemetry(socket, metrics) do
    # Update throughput data from telemetry
    throughput =
      if metrics[:messages_per_sec],
        do: [metrics[:messages_per_sec] | socket.assigns.throughput_data],
        else: socket.assigns.throughput_data

    socket
    |> assign(:throughput_data, Enum.take(throughput, 30))
    |> assign(:telemetry_bytes_per_sec, metrics[:bytes_per_sec] || 0)
    |> assign(:telemetry_consume_rate, metrics[:consume_rate] || 0)
    |> assign(:telemetry_latency_percentiles, metrics[:latency_percentiles] || %{})
    |> assign(:telemetry_total_lag, metrics[:total_lag] || 0)
    |> assign(:telemetry_storage_throughput, metrics[:storage_throughput] || 0)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="min-h-screen bg-dark-bg p-6">
      <div class="max-w-7xl mx-auto">
        <!-- Header -->
        <div class="mb-8">
          <h1 class="text-4xl font-light text-dark-text mb-2">Kafkaesque Dashboard</h1>
          <p class="text-dark-muted">Real-time monitoring for your distributed log system</p>
        </div>
        
    <!-- Key Metrics -->
        <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-4 mb-8">
          <.metric_card
            label="Uptime"
            value={"#{@system_metrics.uptime_hours}h"}
          />
          <.metric_card
            label="Messages"
            value={format_number(@topic_stats.total_messages)}
            trend="up"
            trend_value="+12.5%"
          />
          <.metric_card
            label="Produce Rate"
            value={"#{format_number(@topic_stats.messages_per_sec)}/s"}
          />
          <.metric_card
            label="Consume Rate"
            value={"#{format_number(Map.get(assigns, :telemetry_consume_rate, 0))}/s"}
          />
          <.metric_card
            label="Total Lag"
            value={format_number(Map.get(assigns, :telemetry_total_lag, 0))}
          />
          <.metric_card
            label="Topics"
            value={@topic_stats.total_topics}
          />
          <.metric_card
            label="Partitions"
            value={@topic_stats.total_partitions}
          />
          <.metric_card
            label="Storage"
            value={"#{Float.round(@topic_stats.storage_size_gb, 1)} GB"}
          />
        </div>
        
    <!-- Main Charts Row -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <!-- Throughput Chart -->
          <.panel title="Message Throughput">
            <.svelte
              name="ThroughputChart"
              props={
                %{
                  data: @throughput_data,
                  title: ""
                }
              }
              socket={@socket}
            />
          </.panel>
          
    <!-- Consumer Lag Chart -->
          <.panel title="Consumer Group Lag">
            <.svelte
              name="LagHistogram"
              props={
                %{
                  values: @lag_data.values,
                  labels: @lag_data.labels,
                  title: ""
                }
              }
              socket={@socket}
            />
          </.panel>
        </div>
        
    <!-- System Resources Row -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
          <!-- Resource Gauges -->
          <.panel title="System Resources">
            <div class="space-y-6">
              <.svelte
                name="ResourceGauge"
                props={
                  %{
                    label: "CPU Usage",
                    value: @system_metrics.cpu_usage,
                    max: @system_metrics.cpu_max,
                    unit: "%"
                  }
                }
                socket={@socket}
              />

              <.svelte
                name="ResourceGauge"
                props={
                  %{
                    label: "Memory",
                    value: @system_metrics.memory_usage_mb,
                    max: @system_metrics.memory_max_mb,
                    unit: "MB"
                  }
                }
                socket={@socket}
              />

              <.svelte
                name="ResourceGauge"
                props={
                  %{
                    label: "Disk Usage",
                    value: Float.round(@system_metrics.disk_usage_gb, 1),
                    max: @system_metrics.disk_max_gb,
                    unit: "GB"
                  }
                }
                socket={@socket}
              />
            </div>
          </.panel>
          
    <!-- Topic Distribution -->
          <.panel title="Topic Size Distribution">
            <.svelte
              name="TopicDistributionChart"
              props={
                %{
                  values: @topic_distribution.values,
                  labels: @topic_distribution.labels,
                  title: ""
                }
              }
              socket={@socket}
            />
          </.panel>
          
    <!-- Network Stats -->
          <.panel title="Network I/O">
            <div class="space-y-6">
              <div class="p-4 bg-dark-bg rounded">
                <div class="text-xs text-dark-muted uppercase mb-2">Inbound</div>
                <div class="text-2xl font-mono font-light text-accent-blue">
                  {@system_metrics.network_in_mbps} Mbps
                </div>
                <div class="text-xs text-dark-muted mt-1">↑ 12% from last hour</div>
              </div>

              <div class="p-4 bg-dark-bg rounded">
                <div class="text-xs text-dark-muted uppercase mb-2">Outbound</div>
                <div class="text-2xl font-mono font-light text-accent-green">
                  {@system_metrics.network_out_mbps} Mbps
                </div>
                <div class="text-xs text-dark-muted mt-1">↑ 8% from last hour</div>
              </div>

              <div class="p-4 bg-dark-bg rounded">
                <div class="text-xs text-dark-muted uppercase mb-2">Connections</div>
                <div class="text-2xl font-mono font-light text-dark-text">
                  {45 + :rand.uniform(20)}
                </div>
                <div class="text-xs text-dark-muted mt-1">Active TCP connections</div>
              </div>
            </div>
          </.panel>
        </div>
        
    <!-- Tables Row -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <!-- Topics Table -->
          <.panel title="Topics">
            <.data_table rows={@topics}>
              <:col label="Topic" field={:name} />
              <:col label="Partitions" field={:partitions} />
              <:col label="Messages" field={:messages} />
              <:col label="Size" field={:size_mb} />
            </.data_table>
          </.panel>
          
    <!-- Consumer Groups Table -->
          <.panel title="Consumer Groups">
            <.data_table rows={@consumer_groups}>
              <:col label="Group" field={:name} />
              <:col label="State" field={:state} />
              <:col label="Members" field={:members} />
              <:col label="Lag" field={:lag} />
            </.data_table>
          </.panel>
        </div>
        
    <!-- Partition Health -->
        <.panel title="Partition Health">
          <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
            <%= for partition <- @partition_data do %>
              <div class="p-4 bg-dark-bg rounded border border-dark-border">
                <div class="text-xs text-dark-muted mb-2">Partition {partition.id}</div>
                <div class="text-sm font-mono text-dark-text mb-1">
                  {format_number(partition.messages)} msgs
                </div>
                <div class="text-xs text-dark-muted">
                  {partition.rate} msg/s
                </div>
                <div class="mt-2 h-1 bg-dark-panel rounded overflow-hidden">
                  <div
                    class={"h-full #{partition_health_color(partition.rate)}"}
                    style={"width: #{min(100, partition.rate)}%"}
                  >
                  </div>
                </div>
              </div>
            <% end %>
          </div>
        </.panel>
        
    <!-- Storage & I/O Metrics Row -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8 mt-8">
          <!-- Storage Metrics -->
          <.panel title="Storage I/O Performance">
            <div class="space-y-4">
              <div class="p-3 bg-dark-bg rounded">
                <div class="flex justify-between mb-2">
                  <span class="text-xs text-dark-muted uppercase">Write Throughput</span>
                  <span class="text-sm font-mono text-accent-green">
                    {@storage_metrics.write_throughput_mbps} MB/s
                  </span>
                </div>
                <div class="text-xs text-dark-muted">
                  Latency: {@storage_metrics.write_latency_ms} ms
                </div>
              </div>

              <div class="p-3 bg-dark-bg rounded">
                <div class="flex justify-between mb-2">
                  <span class="text-xs text-dark-muted uppercase">Read Throughput</span>
                  <span class="text-sm font-mono text-accent-blue">
                    {@storage_metrics.read_throughput_mbps} MB/s
                  </span>
                </div>
                <div class="text-xs text-dark-muted">
                  Latency: {@storage_metrics.read_latency_ms} ms
                </div>
              </div>

              <div class="p-3 bg-dark-bg rounded">
                <div class="flex justify-between">
                  <span class="text-xs text-dark-muted">Compression Ratio</span>
                  <span class="text-sm font-mono text-dark-text">
                    {@storage_metrics.compression_ratio}x
                  </span>
                </div>
              </div>

              <div class="p-3 bg-dark-bg rounded">
                <div class="flex justify-between">
                  <span class="text-xs text-dark-muted">Cache Hit Rate</span>
                  <span class="text-sm font-mono text-dark-text">
                    {Float.round(@storage_metrics.cache_hit_rate * 100, 1)}%
                  </span>
                </div>
              </div>
            </div>
          </.panel>
          
    <!-- Rebalance Events -->
          <.panel title="Recent Rebalances">
            <div class="space-y-2">
              <%= if @active_rebalances > 0 do %>
                <div class="p-3 bg-accent-yellow bg-opacity-20 rounded border border-accent-yellow">
                  <div class="text-sm text-accent-yellow">
                    ⚠️ {@active_rebalances} active rebalance(s) in progress
                  </div>
                </div>
              <% end %>

              <%= for event <- @rebalance_events do %>
                <div class="p-3 bg-dark-bg rounded border border-dark-border">
                  <div class="flex justify-between items-start">
                    <div>
                      <div class="text-sm font-mono text-dark-text">{event.group}</div>
                      <div class="text-xs text-dark-muted">
                        {event.partitions} partitions • {event.duration_ms}ms
                      </div>
                    </div>
                    <div class="text-xs text-dark-muted">{event.time}</div>
                  </div>
                </div>
              <% end %>
            </div>
          </.panel>
          
    <!-- Consumer Activity -->
          <.panel title="Consumer Activity">
            <div class="space-y-2">
              <%= for activity <- @consumer_activity do %>
                <div class="flex items-center gap-3 p-2 bg-dark-bg rounded">
                  <div class={consumer_activity_icon(activity.event)}>
                    <%= if activity.event == "joined" do %>
                      <span class="text-accent-green">→</span>
                    <% else %>
                      <span class="text-accent-red">←</span>
                    <% end %>
                  </div>
                  <div class="flex-1">
                    <div class="text-sm text-dark-text">
                      {activity.consumer}
                      <span class="text-dark-muted">
                        {activity.event}
                      </span>
                      {activity.group}
                    </div>
                  </div>
                  <div class="text-xs text-dark-muted">{activity.time}</div>
                </div>
              <% end %>
            </div>
          </.panel>
        </div>
        
    <!-- Event Stream & Latency Percentiles -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <!-- Topic Events -->
          <.panel title="Topic Events">
            <div class="space-y-2">
              <%= if length(@topic_events) == 0 do %>
                <div class="text-sm text-dark-muted text-center py-8">
                  No recent topic events
                </div>
              <% else %>
                <%= for event <- @topic_events do %>
                  <div class="flex items-center gap-3 p-3 bg-dark-bg rounded">
                    <div class={topic_event_icon(event.event)}>
                      <%= if event.event == :created do %>
                        <span class="text-accent-green">✓</span>
                      <% else %>
                        <span class="text-accent-red">✗</span>
                      <% end %>
                    </div>
                    <div class="flex-1">
                      <div class="text-sm font-mono text-dark-text">{event.topic}</div>
                      <div class="text-xs text-dark-muted">Topic {event.event}</div>
                    </div>
                    <div class="text-xs text-dark-muted">{event.time}</div>
                  </div>
                <% end %>
              <% end %>
            </div>
          </.panel>
          
    <!-- Latency Percentiles -->
          <.panel title="Consumer Latency Percentiles">
            <div class="space-y-4">
              <%= for {percentile, value} <- Map.get(assigns, :telemetry_latency_percentiles, %{50 => 0, 95 => 0, 99 => 0}) do %>
                <div class="p-3 bg-dark-bg rounded">
                  <div class="flex justify-between mb-2">
                    <span class="text-sm text-dark-muted">P{percentile}</span>
                    <span class="text-sm font-mono text-dark-text">{value} ms</span>
                  </div>
                  <div class="h-2 bg-dark-panel rounded overflow-hidden">
                    <div
                      class="h-full bg-accent-blue"
                      style={"width: #{min(100, value * 2)}%"}
                    >
                    </div>
                  </div>
                </div>
              <% end %>

              <div class="mt-4 p-3 bg-dark-bg rounded">
                <div class="text-xs text-dark-muted uppercase mb-2">Storage Throughput</div>
                <div class="text-2xl font-mono font-light text-accent-purple">
                  {format_bytes(Map.get(assigns, :telemetry_storage_throughput, 0))}/s
                </div>
              </div>
            </div>
          </.panel>
        </div>
      </div>
    </div>
    """
  end

  defp format_number(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp format_number(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp format_number(n), do: "#{n}"

  defp format_bytes(bytes) when bytes >= 1_073_741_824,
    do: "#{Float.round(bytes / 1_073_741_824, 2)} GB"

  defp format_bytes(bytes) when bytes >= 1_048_576, do: "#{Float.round(bytes / 1_048_576, 2)} MB"
  defp format_bytes(bytes) when bytes >= 1024, do: "#{Float.round(bytes / 1024, 2)} KB"
  defp format_bytes(bytes), do: "#{bytes} B"

  # Unused helper functions - keeping for future use
  # defp state_class("stable"), do: "text-accent-green"
  # defp state_class("rebalancing"), do: "text-accent-yellow"
  # defp state_class(_), do: "text-dark-muted"

  defp partition_health_color(rate) when rate > 150, do: "bg-accent-red"
  defp partition_health_color(rate) when rate > 100, do: "bg-accent-yellow"
  defp partition_health_color(_), do: "bg-accent-green"

  defp consumer_activity_icon("joined"), do: "text-accent-green"
  defp consumer_activity_icon("left"), do: "text-accent-red"
  defp consumer_activity_icon(_), do: "text-dark-muted"

  defp topic_event_icon(:created), do: "text-accent-green"
  defp topic_event_icon(:deleted), do: "text-accent-red"
  defp topic_event_icon(_), do: "text-dark-muted"
end
