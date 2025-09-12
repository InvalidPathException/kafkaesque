defmodule KafkaesqueDashboard.ConsumerGroupLive do
  use Phoenix.LiveView
  import KafkaesqueDashboard.CoreComponents

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(2000, self(), :update_metrics)
    end

    socket =
      socket
      |> assign(:active_tab, :consumer_groups)
      |> assign(:selected_group, nil)
      |> assign(:filter, "")
      |> assign_consumer_group_data()

    {:ok, socket}
  end

  @impl true
  def handle_info(:update_metrics, socket) do
    {:noreply, assign_consumer_group_data(socket)}
  end

  @impl true
  def handle_event("filter", %{"filter" => filter}, socket) do
    {:noreply, assign(socket, :filter, filter)}
  end

  @impl true
  def handle_event("select_group", %{"group" => group_id}, socket) do
    {:noreply,
     socket
     |> assign(:selected_group, group_id)
     |> assign_group_details(group_id)}
  end

  defp assign_consumer_group_data(socket) do
    socket
    |> assign(:consumer_groups, get_consumer_groups())
    |> assign(:total_stats, get_total_stats())
  end

  defp assign_group_details(socket, group_id) do
    socket
    |> assign(:group_details, get_group_details(group_id))
    |> assign(:partition_assignments, get_partition_assignments(group_id))
    |> assign(:lag_history, get_lag_history(group_id))
  end

  defp get_consumer_groups do
    [
      %{
        id: "analytics-pipeline",
        state: "stable",
        members: 5,
        topics: ["user-events", "order-transactions"],
        total_lag: 1234,
        messages_per_sec: 567,
        last_commit: "2 sec ago"
      },
      %{
        id: "event-processor",
        state: "stable",
        members: 3,
        topics: ["analytics-events"],
        total_lag: 0,
        messages_per_sec: 234,
        last_commit: "1 sec ago"
      },
      %{
        id: "backup-writer",
        state: "rebalancing",
        members: 1,
        topics: ["system-logs", "user-profiles"],
        total_lag: 45_678,
        messages_per_sec: 89,
        last_commit: "15 sec ago"
      },
      %{
        id: "stream-aggregator",
        state: "stable",
        members: 2,
        topics: ["order-transactions", "analytics-events"],
        total_lag: 890,
        messages_per_sec: 1234,
        last_commit: "1 sec ago"
      },
      %{
        id: "data-exporter",
        state: "empty",
        members: 0,
        topics: [],
        total_lag: 0,
        messages_per_sec: 0,
        last_commit: "never"
      }
    ]
  end

  defp get_total_stats do
    %{
      total_groups: 5,
      active_groups: 4,
      total_members: 11,
      total_lag: 47_836
    }
  end

  defp get_group_details(group_id) do
    %{
      id: group_id,
      coordinator: "node-0",
      protocol: "range",
      protocol_type: "consumer",
      generation_id: 42,
      leader_id: "consumer-#{group_id}-1",
      session_timeout_ms: 10_000,
      rebalance_timeout_ms: 300_000
    }
  end

  defp get_partition_assignments(group_id) do
    [
      %{
        member_id: "consumer-#{group_id}-1",
        client_id: "client-1",
        host: "192.168.1.10",
        topic: "user-events",
        partitions: [0, 1, 2],
        lag: 234,
        committed_offset: 12_345,
        end_offset: 12_579
      },
      %{
        member_id: "consumer-#{group_id}-2",
        client_id: "client-2",
        host: "192.168.1.11",
        topic: "user-events",
        partitions: [3, 4],
        lag: 567,
        committed_offset: 23_456,
        end_offset: 24_023
      },
      %{
        member_id: "consumer-#{group_id}-3",
        client_id: "client-3",
        host: "192.168.1.12",
        topic: "order-transactions",
        partitions: [0, 1, 2, 3],
        lag: 89,
        committed_offset: 34_567,
        end_offset: 34_656
      }
    ]
  end

  defp get_lag_history(_group_id) do
    for _i <- 0..19 do
      500 + :rand.uniform(200) - 100
    end
  end

  defp filtered_groups(groups, filter) do
    if filter == "" do
      groups
    else
      Enum.filter(groups, fn group ->
        String.contains?(String.downcase(group.id), String.downcase(filter))
      end)
    end
  end

  # Unused helper functions - keeping for future use
  # defp state_color("stable"), do: "text-accent-green"
  # defp state_color("rebalancing"), do: "text-accent-yellow"
  # defp state_color("empty"), do: "text-dark-muted"
  # defp state_color(_), do: "text-dark-text"

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <!-- Page Header -->
      <div class="mb-8">
        <h2 class="text-3xl font-light text-dark-text">Consumer Groups</h2>
        <p class="text-sm text-dark-muted mt-1">Monitor consumer group status and lag</p>
      </div>
      
    <!-- Summary Stats -->
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <.metric_card label="Total Groups" value={@total_stats.total_groups} />
        <.metric_card label="Active Groups" value={@total_stats.active_groups} />
        <.metric_card label="Total Members" value={@total_stats.total_members} />
        <.metric_card
          label="Total Lag"
          value={@total_stats.total_lag}
          trend={if @total_stats.total_lag > 10000, do: "up"}
          trend_value={if @total_stats.total_lag > 10000, do: "+23%"}
        />
      </div>
      
    <!-- Lag Overview Chart -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <.panel title="Consumer Group Lag">
          <div class="h-64">
            <canvas
              id="consumer-lag-chart"
              phx-hook="LagChart"
              data-values={Jason.encode!(Enum.map(@consumer_groups, & &1.total_lag))}
              data-labels={Jason.encode!(Enum.map(@consumer_groups, & &1.id))}
            >
            </canvas>
          </div>
        </.panel>
        <.panel title="Messages per Second">
          <div class="h-64">
            <canvas
              id="consumer-rate-chart"
              phx-hook="LagChart"
              data-values={Jason.encode!(Enum.map(@consumer_groups, & &1.messages_per_sec))}
              data-labels={Jason.encode!(Enum.map(@consumer_groups, & &1.id))}
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
            placeholder="Filter consumer groups..."
            class="input-dark bg-dark-panel border border-dark-border rounded-md px-4 py-2 w-full md:w-96"
          />
        </form>
      </div>
      
    <!-- Consumer Groups Table -->
      <.panel title="All Consumer Groups">
        <.data_table rows={filtered_groups(@consumer_groups, @filter)}>
          <:col label="Group ID" field={:id} />
          <:col label="State" field={:state} />
          <:col label="Members" field={:members} />
          <:col label="Topics" field={:topics} />
          <:col label="Total Lag" field={:total_lag} />
          <:col label="Rate" field={:messages_per_sec} />
          <:col label="Last Commit" field={:last_commit} />
        </.data_table>
      </.panel>
      
    <!-- Group Details Modal/Panel -->
      <%= if @selected_group do %>
        <div class="mt-8">
          <div class="mb-6 border-t border-dark-border pt-6">
            <h3 class="text-2xl font-light text-dark-text mb-2">Group Details: {@selected_group}</h3>
          </div>
          
    <!-- Group Configuration -->
          <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
            <.panel title="Configuration">
              <dl class="space-y-3">
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Coordinator</dt>
                  <dd class="text-sm text-dark-text font-mono">{@group_details.coordinator}</dd>
                </div>
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Protocol</dt>
                  <dd class="text-sm text-dark-text font-mono">{@group_details.protocol}</dd>
                </div>
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Generation ID</dt>
                  <dd class="text-sm text-dark-text font-mono">{@group_details.generation_id}</dd>
                </div>
                <div>
                  <dt class="text-xs text-dark-muted uppercase">Session Timeout</dt>
                  <dd class="text-sm text-dark-text font-mono">
                    {@group_details.session_timeout_ms} ms
                  </dd>
                </div>
              </dl>
            </.panel>
            
    <!-- Lag Trend Chart -->
            <div class="lg:col-span-2">
              <.panel title="Lag Trend (Last 20 min)">
                <div class="h-48">
                  <canvas
                    id="lag-trend-chart"
                    phx-hook="ThroughputChart"
                    data-values={Jason.encode!(@lag_history)}
                  >
                  </canvas>
                </div>
              </.panel>
            </div>
          </div>
          
    <!-- Partition Assignments -->
          <.panel title="Partition Assignments">
            <.data_table rows={@partition_assignments}>
              <:col label="Member ID" field={:member_id} />
              <:col label="Client ID" field={:client_id} />
              <:col label="Host" field={:host} />
              <:col label="Topic" field={:topic} />
              <:col label="Partitions" field={:partitions} />
              <:col label="Lag" field={:lag} />
              <:col label="Progress">
                <.progress_bar
                  value={@committed_offset}
                  max={@end_offset}
                  color="green"
                />
              </:col>
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
end
