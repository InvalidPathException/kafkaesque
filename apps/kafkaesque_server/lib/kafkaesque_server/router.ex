defmodule KafkaesqueServer.Router do
  use Phoenix.Router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/v1", KafkaesqueServer do
    pipe_through :api

    # Topics
    post "/topics", TopicController, :create
    get "/topics", TopicController, :index

    # Records
    post "/topics/:topic/records", RecordController, :produce
    get "/topics/:topic/records", RecordController, :consume

    # Offsets
    post "/consumers/:group/offsets", OffsetController, :commit
    get "/topics/:topic/offsets", OffsetController, :get
  end
end
