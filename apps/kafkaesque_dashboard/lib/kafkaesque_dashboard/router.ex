defmodule KafkaesqueDashboard.Router do
  use KafkaesqueDashboard, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {KafkaesqueDashboard.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug :put_layout, html: {KafkaesqueDashboard.Layouts, :app}
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", KafkaesqueDashboard do
    pipe_through :browser

    live "/", OverviewLive, :index
    live "/topics", TopicLive, :index
    live "/topics/:topic", TopicLive, :show
    live "/consumer-groups", ConsumerGroupLive, :index
  end

  # LiveDashboard
  if Application.compile_env(:kafkaesque_dashboard, :dev_routes) do
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: KafkaesqueDashboard.Telemetry
    end
  end
end
