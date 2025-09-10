defmodule KafkaesqueServer.TopicController do
  use Phoenix.Controller, formats: [:json]

  def create(conn, _params) do
    json(conn, %{status: "not_implemented"})
  end

  def index(conn, _params) do
    json(conn, %{topics: []})
  end
end
