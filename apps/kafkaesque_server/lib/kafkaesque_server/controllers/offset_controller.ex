defmodule KafkaesqueServer.OffsetController do
  use Phoenix.Controller, formats: [:json]

  def commit(conn, _params) do
    json(conn, %{status: "not_implemented"})
  end

  def get(conn, _params) do
    json(conn, %{earliest: 0, latest: 0})
  end
end
