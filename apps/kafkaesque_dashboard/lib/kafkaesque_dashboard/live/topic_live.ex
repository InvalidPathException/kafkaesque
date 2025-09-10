defmodule KafkaesqueDashboard.TopicLive do
  use Phoenix.LiveView

  @impl true
  def mount(_params, _session, socket) do
    {:ok, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <h1>Topics</h1>
      <p>Topics list coming soon...</p>
    </div>
    """
  end
end
