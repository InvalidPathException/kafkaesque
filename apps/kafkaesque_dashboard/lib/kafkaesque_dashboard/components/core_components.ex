defmodule KafkaesqueDashboard.CoreComponents do
  @moduledoc """
  Core UI components for Kafkaesque Dashboard.
  """
  use Phoenix.Component

  @doc """
  Renders a metric card with a large value display.
  """
  attr :label, :string, required: true
  attr :value, :any, required: true
  attr :unit, :string, default: nil
  attr :trend, :string, default: nil
  attr :trend_value, :string, default: nil
  attr :class, :string, default: ""

  def metric_card(assigns) do
    ~H"""
    <div class={"panel p-6 #{@class}"}>
      <div class="text-xs text-dark-muted uppercase tracking-wider mb-2">
        {@label}
      </div>
      <div class="flex items-baseline">
        <span class="metric text-dark-text">
          {format_metric(@value)}
        </span>
        <%= if @unit do %>
          <span class="text-lg text-dark-muted ml-2">
            {@unit}
          </span>
        <% end %>
      </div>
      <%= if @trend do %>
        <div class="mt-3 flex items-center text-xs">
          <%= if @trend == "up" do %>
            <svg class="w-4 h-4 text-accent-green mr-1" fill="currentColor" viewBox="0 0 20 20">
              <path
                fill-rule="evenodd"
                d="M5.293 9.707a1 1 0 010-1.414l4-4a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L11 7.414V15a1 1 0 11-2 0V7.414L6.707 9.707a1 1 0 01-1.414 0z"
                clip-rule="evenodd"
              />
            </svg>
            <span class="text-accent-green">{@trend_value}</span>
          <% else %>
            <svg class="w-4 h-4 text-accent-red mr-1" fill="currentColor" viewBox="0 0 20 20">
              <path
                fill-rule="evenodd"
                d="M14.707 10.293a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 111.414-1.414L9 12.586V5a1 1 0 012 0v7.586l2.293-2.293a1 1 0 011.414 0z"
                clip-rule="evenodd"
              />
            </svg>
            <span class="text-accent-red">{@trend_value}</span>
          <% end %>
          <span class="text-dark-muted ml-1">from last hour</span>
        </div>
      <% end %>
    </div>
    """
  end

  @doc """
  Renders a panel container.
  """
  attr :title, :string, default: nil
  attr :class, :string, default: ""
  slot :inner_block, required: true
  slot :actions

  def panel(assigns) do
    ~H"""
    <div class={"panel #{@class}"}>
      <%= if @title do %>
        <div class="px-6 py-4 border-b border-dark-border flex items-center justify-between">
          <h3 class="text-lg font-medium text-dark-text">{@title}</h3>
          <%= if @actions != [] do %>
            <div class="flex items-center space-x-2">
              {render_slot(@actions)}
            </div>
          <% end %>
        </div>
      <% end %>
      <div class="p-6">
        {render_slot(@inner_block)}
      </div>
    </div>
    """
  end

  @doc """
  Renders a data table.
  """
  attr :rows, :list, required: true
  attr :class, :string, default: ""

  slot :col, required: true do
    attr :label, :string
    attr :field, :atom
  end

  def data_table(assigns) do
    ~H"""
    <div class={"overflow-x-auto #{@class}"}>
      <table class="data-table">
        <thead>
          <tr>
            <%= for col <- @col do %>
              <th>{col[:label]}</th>
            <% end %>
          </tr>
        </thead>
        <tbody>
          <%= for row <- @rows do %>
            <tr>
              <%= for col <- @col do %>
                <td>
                  <%= if col[:field] do %>
                    {format_value(Map.get(row, col[:field]))}
                  <% else %>
                    {render_slot(col, row)}
                  <% end %>
                </td>
              <% end %>
            </tr>
          <% end %>
        </tbody>
      </table>
    </div>
    """
  end

  @doc """
  Renders a status badge.
  """
  attr :status, :string, required: true
  attr :label, :string, default: nil

  def status_badge(assigns) do
    ~H"""
    <div class="inline-flex items-center">
      <span class={"status-dot #{status_class(@status)} mr-2"}></span>
      <span class="text-sm text-dark-text">
        {@label || String.capitalize(@status)}
      </span>
    </div>
    """
  end

  @doc """
  Renders a chart container with placeholder.
  """
  attr :id, :string, required: true
  attr :title, :string, default: nil
  attr :height, :string, default: "h-64"
  attr :class, :string, default: ""

  def chart(assigns) do
    ~H"""
    <div class={"panel #{@class}"}>
      <%= if @title do %>
        <div class="px-6 py-4 border-b border-dark-border">
          <h3 class="text-sm font-medium text-dark-text uppercase tracking-wider">{@title}</h3>
        </div>
      <% end %>
      <div class={"relative #{@height} p-6"} id={@id} phx-hook="Chart">
        <div class="absolute inset-0 flex items-center justify-center">
          <div class="text-dark-muted text-sm">
            Chart Loading...
          </div>
        </div>
      </div>
    </div>
    """
  end

  @doc """
  Renders a progress bar.
  """
  attr :value, :integer, required: true
  attr :max, :integer, default: 100
  attr :label, :string, default: nil
  attr :color, :string, default: "blue"

  def progress_bar(assigns) do
    percentage = min(100, assigns.value / assigns.max * 100)
    assigns = assign(assigns, :percentage, percentage)

    ~H"""
    <div>
      <%= if @label do %>
        <div class="flex justify-between text-sm mb-1">
          <span class="text-dark-muted">{@label}</span>
          <span class="text-dark-text font-mono">{@value}/{@max}</span>
        </div>
      <% end %>
      <div class="w-full bg-dark-panel rounded-full h-2 overflow-hidden">
        <div
          class={"h-full transition-all duration-300 #{progress_color(@color)}"}
          style={"width: #{@percentage}%"}
        />
      </div>
    </div>
    """
  end

  @doc """
  Renders a mini sparkline chart.
  """
  attr :data, :list, required: true
  attr :color, :string, default: "blue"
  attr :height, :integer, default: 40
  attr :width, :integer, default: 120

  def sparkline(assigns) do
    max_value = Enum.max(assigns.data, fn -> 1 end)

    points =
      assigns.data
      |> Enum.with_index()
      |> Enum.map_join(" ", fn {value, index} ->
        x = index * (assigns.width / (length(assigns.data) - 1))
        y = assigns.height - value / max_value * assigns.height
        "#{x},#{y}"
      end)

    assigns = assign(assigns, :points, points)

    ~H"""
    <svg width={@width} height={@height} class="inline-block">
      <polyline
        fill="none"
        stroke={sparkline_color(@color)}
        stroke-width="2"
        points={@points}
      />
    </svg>
    """
  end

  # Helper functions
  defp format_metric(value) when is_integer(value) do
    cond do
      value >= 1_000_000_000 -> "#{Float.round(value / 1_000_000_000, 1)}B"
      value >= 1_000_000 -> "#{Float.round(value / 1_000_000, 1)}M"
      value >= 1_000 -> "#{Float.round(value / 1_000, 1)}K"
      true -> "#{value}"
    end
  end

  defp format_metric(value) when is_float(value) do
    "#{Float.round(value, 2)}"
  end

  defp format_metric(value), do: "#{value}"

  defp status_class("healthy"), do: "status-healthy"
  defp status_class("warning"), do: "status-warning"
  defp status_class("error"), do: "status-error"
  defp status_class("active"), do: "status-healthy"
  defp status_class("inactive"), do: "status-warning"
  defp status_class(_), do: "bg-dark-muted"

  defp format_value(nil), do: ""
  defp format_value(value) when is_list(value), do: Enum.join(value, ", ")

  defp format_value(value) when is_number(value) and value >= 1_000_000,
    do: "#{Float.round(value / 1_000_000, 1)}M"

  defp format_value(value) when is_number(value) and value >= 1_000,
    do: "#{Float.round(value / 1_000, 1)}K"

  defp format_value(value), do: to_string(value)

  defp sparkline_color("blue"), do: "#1890ff"
  defp sparkline_color("green"), do: "#52c41a"
  defp sparkline_color("red"), do: "#ff4d4f"
  defp sparkline_color(_), do: "#8c8c8c"

  defp progress_color("green"), do: "bg-accent-green"
  defp progress_color("yellow"), do: "bg-accent-yellow"
  defp progress_color("red"), do: "bg-accent-red"
  defp progress_color("blue"), do: "bg-accent-blue"
  defp progress_color("purple"), do: "bg-accent-purple"
  defp progress_color(_), do: "bg-accent-blue"
end
