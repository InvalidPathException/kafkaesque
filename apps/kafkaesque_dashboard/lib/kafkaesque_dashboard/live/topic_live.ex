defmodule KafkaesqueDashboard.TopicLive do
  use Phoenix.LiveView
  import KafkaesqueDashboard.CoreComponents

  @impl true
  def mount(params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(3000, self(), :update_metrics)
    end

    socket =
      socket
      |> assign(:active_tab, :topics)
      |> assign(:selected_topic, params["topic"])
      |> assign(:filter, "")
      |> assign_topic_data()

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"topic" => topic}, _uri, socket) do
    {:noreply,
     socket
     |> assign(:selected_topic, topic)
     |> assign_topic_details(topic)}
  end

  def handle_params(_params, _uri, socket) do
    {:noreply, assign(socket, :selected_topic, nil)}
  end

  @impl true
  def handle_info(:update_metrics, socket) do
    {:noreply, assign_topic_data(socket)}
  end

  @impl true
  def handle_event("filter", %{"filter" => filter}, socket) do
    {:noreply, assign(socket, :filter, filter)}
  end

  defp assign_topic_data(socket) do
    socket
    |> assign(:topics, get_topics())
    |> assign(:total_stats, get_total_stats())
  end

  defp assign_topic_details(socket, topic_name) do
    socket
    |> assign(:topic_details, get_topic_details(topic_name))
    |> assign(:partition_data, get_partition_data(topic_name))
    |> assign(:consumer_groups_for_topic, get_consumer_groups_for_topic(topic_name))
    |> assign(:topic_throughput_data, get_topic_throughput_data())
    |> assign(:partition_distribution_data, get_partition_distribution_data(topic_name))
  end

  defp get_topics do
    [
      %{
        name: "user-events",
        partitions: 8,
        replication_factor: 1,
        messages: 523_456,
        bytes: 1_234_567,
        messages_per_sec: 234,
        bytes_per_sec: 45_678,
        retention_ms: 604_800_000
      },
      %{
        name: "order-transactions",
        partitions: 12,
        replication_factor: 1,
        messages: 1_234_567,
        bytes: 5_678_912,
        messages_per_sec: 567,
        bytes_per_sec: 123_456,
        retention_ms: 604_800_000
      },
      %{
        name: "analytics-events",
        partitions: 4,
        replication_factor: 1,
        messages: 89_012,
        bytes: 234_567,
        messages_per_sec: 45,
        bytes_per_sec: 8_901,
        retention_ms: 259_200_000
      },
      %{
        name: "system-logs",
        partitions: 2,
        replication_factor: 1,
        messages: 2_345_678,
        bytes: 8_901_234,
        messages_per_sec: 890,
        bytes_per_sec: 234_567,
        retention_ms: 86_400_000
      },
      %{
        name: "user-profiles",
        partitions: 6,
        replication_factor: 1,
        messages: 45_678,
        bytes: 567_890,
        messages_per_sec: 12,
        bytes_per_sec: 3_456,
        retention_ms: 2_592_000_000
      }
    ]
  end

  defp get_total_stats do
    %{
      total_topics: 5,
      total_partitions: 32,
      total_messages: 4_236_369,
      total_bytes: 16_637_170
    }
  end

  defp get_topic_details(topic_name) do
    %{
      name: topic_name,
      created_at: "2024-01-15 10:30:00",
      min_in_sync_replicas: 1,
      compression_type: "snappy",
      cleanup_policy: "delete",
      segment_ms: 604_800_000,
      segment_bytes: 1_073_741_824
    }
  end

  defp get_partition_data(_topic_name) do
    for i <- 0..7 do
      %{
        id: i,
        leader: 0,
        replicas: [0],
        isr: [0],
        start_offset: i * 10_000,
        end_offset: i * 10_000 + :rand.uniform(5000),
        size_bytes: :rand.uniform(10_000_000),
        messages_per_sec: :rand.uniform(100)
      }
    end
  end

  defp get_consumer_groups_for_topic(_topic_name) do
    [
      %{name: "analytics-pipeline", lag: 234},
      %{name: "event-processor", lag: 0},
      %{name: "backup-writer", lag: 1567}
    ]
  end

  defp get_topic_throughput_data do
    for _i <- 0..19 do
      200 + :rand.uniform(100)
    end
  end

  defp get_partition_distribution_data(topic_name) do
    partitions = get_partition_data(topic_name)

    %{
      values: Enum.map(partitions, & &1.size_bytes),
      labels: Enum.map(partitions, &"Partition #{&1.id}")
    }
  end

  defp filtered_topics(topics, filter) do
    if filter == "" do
      topics
    else
      Enum.filter(topics, fn topic ->
        String.contains?(String.downcase(topic.name), String.downcase(filter))
      end)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <%= if @selected_topic do %>
        <!-- Topic Detail View -->
        <div>
          <!-- Breadcrumb -->
          <div class="mb-6">
            <nav class="flex text-sm">
              <.link navigate="/topics" class="text-accent-blue hover:text-graph-blue">
                Topics
              </.link>
              <span class="mx-2 text-dark-muted">/</span>
              <span class="text-dark-text">{@selected_topic}</span>
            </nav>
          </div>
          
    <!-- Topic Header -->
          <div class="mb-8">
            <h2 class="text-3xl font-light text-dark-text">{@selected_topic}</h2>
            <p class="text-sm text-dark-muted mt-1">Created {@topic_details.created_at}</p>
          </div>
          
    <!-- Topic Metrics -->
          <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
            <.metric_card label="Partitions" value={length(@partition_data)} />
            <.metric_card
              label="Total Messages"
              value={Enum.sum(Enum.map(@partition_data, & &1.end_offset))}
            />
            <.metric_card
              label="Size"
              value={Float.round(Enum.sum(Enum.map(@partition_data, & &1.size_bytes)) / 1_048_576, 2)}
              unit="MB"
            />
            <.metric_card
              label="Throughput"
              value={Enum.sum(Enum.map(@partition_data, & &1.messages_per_sec))}
              unit="msg/s"
            />
          </div>
          
    <!-- Charts Row -->
          <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <!-- Throughput Chart -->
            <.panel title="Throughput (msg/sec)">
              <div class="h-64">
                <canvas
                  id="topic-throughput-chart"
                  phx-hook="ThroughputChart"
                  data-values={Jason.encode!(@topic_throughput_data)}
                >
                </canvas>
              </div>
            </.panel>
            
    <!-- Partition Distribution -->
            <.panel title="Partition Distribution">
              <div class="h-64">
                <canvas
                  id="partition-distribution-chart"
                  phx-hook="TopicChart"
                  data-values={Jason.encode!(@partition_distribution_data.values)}
                  data-labels={Jason.encode!(@partition_distribution_data.labels)}
                >
                </canvas>
              </div>
            </.panel>
          </div>
          
    <!-- Configuration and Partitions -->
          <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
            <!-- Configuration -->
            <.panel title="Configuration">
              <dl class="space-y-3">
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Compression</dt>
                  <dd class="text-sm text-dark-text font-mono">{@topic_details.compression_type}</dd>
                </div>
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Cleanup Policy</dt>
                  <dd class="text-sm text-dark-text font-mono">{@topic_details.cleanup_policy}</dd>
                </div>
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Segment Size</dt>
                  <dd class="text-sm text-dark-text font-mono">
                    {div(@topic_details.segment_bytes, 1_048_576)} MB
                  </dd>
                </div>
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Retention</dt>
                  <dd class="text-sm text-dark-text font-mono">
                    {div(@topic_details.segment_ms, 86_400_000)} days
                  </dd>
                </div>
              </dl>
            </.panel>
            
    <!-- Partitions Table -->
            <div class="lg:col-span-2">
              <.panel title="Partitions">
                <.data_table rows={@partition_data}>
                  <:col label="ID" field={:id} />
                  <:col label="Leader" field={:leader} />
                  <:col label="Start Offset" field={:start_offset} />
                  <:col label="End Offset" field={:end_offset} />
                  <:col label="Size" field={:size_bytes} />
                  <:col label="Rate" field={:messages_per_sec} />
                </.data_table>
              </.panel>
            </div>
          </div>
          
    <!-- Consumer Groups -->
          <.panel title="Consumer Groups">
            <.data_table rows={@consumer_groups_for_topic}>
              <:col label="Group Name" field={:name} />
              <:col label="Lag" field={:lag} />
            </.data_table>
          </.panel>
        </div>
      <% else %>
        <!-- Topics List View -->
        <div>
          <!-- Page Header -->
          <div class="mb-8">
            <h2 class="text-3xl font-light text-dark-text">Topics</h2>
            <p class="text-sm text-dark-muted mt-1">Manage and monitor Kafka topics</p>
          </div>
          
    <!-- Summary Stats -->
          <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
            <.metric_card label="Total Topics" value={@total_stats.total_topics} />
            <.metric_card label="Total Partitions" value={@total_stats.total_partitions} />
            <.metric_card label="Total Messages" value={@total_stats.total_messages} />
            <.metric_card
              label="Total Size"
              value={Float.round(@total_stats.total_bytes / 1_048_576, 2)}
              unit="MB"
            />
          </div>
          
    <!-- Topic Distribution Chart -->
          <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <.panel title="Topic Size Distribution">
              <div class="h-64">
                <canvas
                  id="topic-size-chart"
                  phx-hook="TopicChart"
                  data-values={Jason.encode!(Enum.map(@topics, & &1.bytes))}
                  data-labels={Jason.encode!(Enum.map(@topics, & &1.name))}
                >
                </canvas>
              </div>
            </.panel>
            <.panel title="Message Rate by Topic">
              <div class="h-64">
                <canvas
                  id="topic-rate-chart"
                  phx-hook="LagChart"
                  data-values={Jason.encode!(Enum.map(@topics, & &1.messages_per_sec))}
                  data-labels={Jason.encode!(Enum.map(@topics, & &1.name))}
                >
                </canvas>
              </div>
            </.panel>
          </div>
          
    <!-- Filter -->
          <div class="mb-6">
            <form phx-change="filter">
              <input
                type="text"
                name="filter"
                value={@filter}
                placeholder="Filter topics..."
                class="input-dark bg-dark-panel border border-dark-border rounded-md px-4 py-2 w-full md:w-96"
              />
            </form>
          </div>
          
    <!-- Topics Table -->
          <.panel title="All Topics">
            <.data_table rows={filtered_topics(@topics, @filter)}>
              <:col label="Name" field={:name} />
              <:col label="Partitions" field={:partitions} />
              <:col label="Messages" field={:messages} />
              <:col label="Size" field={:bytes} />
              <:col label="Messages/sec" field={:messages_per_sec} />
              <:col label="Bytes/sec" field={:bytes_per_sec} />
              <:col label="Retention" field={:retention_ms} />
            </.data_table>
          </.panel>
        </div>
      <% end %>
    </div>
    """
  end

  # Unused helper functions - keeping for future use
  # defp format_number(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  # defp format_number(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  # defp format_number(n), do: "#{n}"

  # defp format_retention(ms) do
  #   days = div(ms, 86_400_000)
  #   "#{days}d"
  # end
end
