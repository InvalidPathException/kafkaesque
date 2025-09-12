defmodule KafkaesqueDashboard.OverviewLive do
  use Phoenix.LiveView
  import KafkaesqueDashboard.CoreComponents

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(5000, self(), :update_metrics)
    end

    socket =
      socket
      |> assign(:active_tab, :overview)
      |> assign_metrics()

    {:ok, socket}
  end

  @impl true
  def handle_info(:update_metrics, socket) do
    {:noreply, assign_metrics(socket)}
  end

  defp assign_metrics(socket) do
    socket
    |> assign(:system_metrics, get_system_metrics())
    |> assign(:topic_stats, get_topic_stats())
    |> assign(:consumer_groups, get_consumer_groups())
    |> assign(:throughput_data, get_throughput_data())
    |> assign(:recent_activity, get_recent_activity())
  end

  defp get_system_metrics do
    {uptime_seconds, _} = :erlang.statistics(:wall_clock)
    memory = :erlang.memory()

    %{
      uptime_hours: div(uptime_seconds, 3_600_000),
      memory_usage_mb: div(memory[:total], 1024 * 1024),
      cpu_usage: :rand.uniform(100),
      beam_processes: :erlang.system_info(:process_count),
      io_input: :rand.uniform(1_000_000),
      io_output: :rand.uniform(1_000_000)
    }
  end

  defp get_topic_stats do
    %{
      total_topics: 12,
      total_partitions: 48,
      total_messages: 1_234_567,
      storage_size_gb: 2.4,
      messages_per_sec: 1234,
      bytes_per_sec: 567_890
    }
  end

  defp get_consumer_groups do
    [
      %{name: "analytics-pipeline", consumers: 5, lag: 123, status: "active"},
      %{name: "event-processor", consumers: 3, lag: 0, status: "active"},
      %{name: "backup-writer", consumers: 1, lag: 4567, status: "warning"},
      %{name: "stream-aggregator", consumers: 2, lag: 89, status: "active"}
    ]
  end

  defp get_throughput_data do
    for _i <- 0..19 do
      1000 + :rand.uniform(500)
    end
  end

  defp get_recent_activity do
    [
      %{time: "2 min ago", event: "Topic 'user-events' created", type: "info"},
      %{time: "5 min ago", event: "Consumer group 'analytics' rebalanced", type: "warning"},
      %{time: "12 min ago", event: "Partition 3 of 'orders' reached retention", type: "info"},
      %{time: "18 min ago", event: "New consumer joined 'event-processor'", type: "info"},
      %{time: "25 min ago", event: "Compaction completed for 'user-profiles'", type: "success"}
    ]
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <!-- Page Header -->
      <div class="mb-8">
        <h2 class="text-3xl font-light text-dark-text">System Overview</h2>
        <p class="text-sm text-dark-muted mt-1">Real-time metrics and system health</p>
      </div>
      
    <!-- Key Metrics Grid -->
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4 mb-8">
        <.metric_card label="Uptime" value={"#{@system_metrics.uptime_hours} hours"} />
        <.metric_card
          label="Messages"
          value={@topic_stats.total_messages}
          trend="up"
          trend_value="+12.5%"
        />
        <.metric_card label="Topics" value={@topic_stats.total_topics} />
        <.metric_card label="Partitions" value={@topic_stats.total_partitions} />
        <.metric_card label="Rate" value={"#{@topic_stats.messages_per_sec} msg/s"} />
        <.metric_card label="Storage" value={"#{@topic_stats.storage_size_gb} GB"} />
      </div>
      
    <!-- Charts Row -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <!-- Throughput Chart -->
        <.panel title="Message Throughput">
          <div class="h-64">
            <canvas
              id="throughput-chart"
              phx-hook="ThroughputChart"
              data-values={Jason.encode!(@throughput_data)}
            >
            </canvas>
          </div>
        </.panel>
        
    <!-- System Resources -->
        <.panel title="System Resources">
          <div class="grid grid-cols-2 gap-4">
            <div>
              <div class="text-center">
                <canvas
                  id="cpu-gauge"
                  phx-hook="GaugeChart"
                  data-value={@system_metrics.cpu_usage}
                  data-max="100"
                  class="h-32"
                >
                </canvas>
                <p class="text-xs text-dark-muted mt-2">CPU Usage</p>
                <p class="text-2xl font-mono font-light">{@system_metrics.cpu_usage}%</p>
              </div>
            </div>
            <div>
              <div class="text-center">
                <canvas
                  id="memory-gauge"
                  phx-hook="GaugeChart"
                  data-value={@system_metrics.memory_usage_mb}
                  data-max="4096"
                  class="h-32"
                >
                </canvas>
                <p class="text-xs text-dark-muted mt-2">Memory</p>
                <p class="text-2xl font-mono font-light">{@system_metrics.memory_usage_mb} MB</p>
              </div>
            </div>
          </div>
          <div class="mt-4 pt-4 border-t border-dark-border grid grid-cols-2 gap-4">
            <div>
              <span class="text-xs text-dark-muted">BEAM Processes</span>
              <p class="text-2xl font-mono font-light">{@system_metrics.beam_processes}</p>
            </div>
            <div>
              <span class="text-xs text-dark-muted">Schedulers</span>
              <p class="text-2xl font-mono font-light">{:erlang.system_info(:schedulers_online)}</p>
            </div>
          </div>
        </.panel>
      </div>
      
    <!-- Consumer Groups and Activity -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Consumer Groups Table -->
        <.panel title="Consumer Groups">
          <.data_table rows={@consumer_groups}>
            <:col label="Group" field={:name} />
            <:col label="Consumers" field={:consumers} />
            <:col label="Lag" field={:lag} />
            <:col label="Status" field={:status} />
          </.data_table>
        </.panel>
        
    <!-- Recent Activity -->
        <.panel title="Recent Activity">
          <div class="space-y-3">
            <%= for activity <- @recent_activity do %>
              <div class="flex items-start space-x-3 pb-3 border-b border-dark-border last:border-0">
                <div class="flex-shrink-0 mt-1">
                  <%= case activity.type do %>
                    <% "success" -> %>
                      <span class="status-dot status-healthy"></span>
                    <% "warning" -> %>
                      <span class="status-dot status-warning"></span>
                    <% "error" -> %>
                      <span class="status-dot status-error"></span>
                    <% _ -> %>
                      <span class="status-dot bg-accent-blue"></span>
                  <% end %>
                </div>
                <div class="flex-1">
                  <p class="text-sm text-dark-text">{activity.event}</p>
                  <p class="text-xs text-dark-muted mt-1">{activity.time}</p>
                </div>
              </div>
            <% end %>
          </div>
        </.panel>
      </div>
    </div>
    """
  end
end
