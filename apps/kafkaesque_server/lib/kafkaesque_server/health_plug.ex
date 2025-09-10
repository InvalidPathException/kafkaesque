defmodule KafkaesqueServer.HealthPlug do
  @moduledoc """
  Simple health check endpoint plug.
  """

  import Plug.Conn

  def init(opts), do: opts

  def call(%Plug.Conn{path_info: ["healthz"]} = conn, _opts) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "OK")
    |> halt()
  end

  def call(conn, _opts), do: conn
end
