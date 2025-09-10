defmodule KafkaesqueDashboard.OverviewLive do
  use Phoenix.LiveView

  @impl true
  def mount(_params, _session, socket) do
    {:ok, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <h1>Kafkaesque Overview</h1>
      <p>Dashboard coming soon...</p>
    </div>
    """
  end
end
