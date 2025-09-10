defmodule KafkaesqueDashboard.ConsumerGroupLive do
  use Phoenix.LiveView

  @impl true
  def mount(_params, _session, socket) do
    {:ok, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <h1>Consumer Groups</h1>
      <p>Consumer groups list coming soon...</p>
    </div>
    """
  end
end
