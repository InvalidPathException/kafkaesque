defmodule KafkaesqueServer.Router do
  use Phoenix.Router

  pipeline :api do
    plug :accepts, ["json"]
  end

  # Health check endpoint (no authentication)
  get "/healthz", KafkaesqueServer.HealthController, :check
  get "/health/detailed", KafkaesqueServer.HealthController, :detailed

  scope "/v1", KafkaesqueServer do
    pipe_through :api

    # Topics
    post "/topics", TopicController, :create
    get "/topics", TopicController, :index
    get "/topics/:topic", TopicController, :show

    # Records
    post "/topics/:topic/records", RecordController, :produce
    get "/topics/:topic/records", RecordController, :consume

    # Offsets
    post "/consumers/:group/offsets", OffsetController, :commit
    get "/topics/:topic/offsets", OffsetController, :get
  end
end
