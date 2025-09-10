defmodule KafkaesqueServer.RecordController do
  use Phoenix.Controller, formats: [:json]

  def produce(conn, _params) do
    json(conn, %{status: "not_implemented"})
  end

  def consume(conn, _params) do
    json(conn, %{records: []})
  end
end
