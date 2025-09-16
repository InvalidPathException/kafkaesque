defmodule Kafkaesque.Test.Client do
  @moduledoc """
  Test client wrappers for HTTP and gRPC testing.
  """

  import Plug.Test
  import Plug.Conn

  alias KafkaesqueServer.{OffsetController, RecordController, TopicController}

  @doc """
  Make an HTTP request using Plug.Test instead of real HTTP.
  This avoids network issues and port conflicts.
  """
  def http_request(method, path, params \\ %{}, headers \\ []) do
    conn = conn(method, path)

    conn =
      Enum.reduce(headers, conn, fn {key, value}, acc ->
        put_req_header(acc, key, value)
      end)

    # Route to appropriate controller based on path
    conn = route_request(conn, method, path, params)

    # Return in consistent format
    {:ok, conn.status, conn.resp_body |> decode_response()}
  end

  defp route_request(conn, :post, path, params) do
    cond do
      path == "/v1/topics" ->
        TopicController.create(conn, params)

      path == "/v1/offsets/commit" ->
        OffsetController.commit(conn, params)

      String.match?(path, ~r{^/v1/topics/.+/records$}) ->
        [_, _, _, topic, _] = String.split(path, "/")
        RecordController.produce(conn, Map.put(params, "topic", topic))

      true ->
        conn |> put_status(404) |> json(%{error: "Not found"})
    end
  end

  defp route_request(conn, :get, path, params) do
    cond do
      path == "/v1/topics" ->
        TopicController.index(conn, params)

      String.match?(path, ~r{^/v1/topics/.+/records$}) ->
        [_, _, _, topic, _] = String.split(path, "/")
        RecordController.consume(conn, Map.put(params, "topic", topic))

      String.match?(path, ~r{^/v1/offsets/.+$}) ->
        [_, _, topic] = String.split(path, "/")
        OffsetController.get(conn, Map.put(params, "topic", topic))

      true ->
        conn |> put_status(404) |> json(%{error: "Not found"})
    end
  end

  defp route_request(conn, _method, _path, _params) do
    conn
    |> put_status(404)
    |> json(%{error: "Not found"})
  end

  defp decode_response(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, json} -> json
      _ -> body
    end
  end

  defp decode_response(body), do: body

  defp json(conn, data) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(conn.status || 200, Jason.encode!(data))
  end

  @doc """
  Simplified gRPC client for testing without full gRPC stack.
  Directly calls the service implementation.
  """
  def grpc_call(service_fun, request) do
    # Call the service function directly, bypassing gRPC transport
    apply(Kafkaesque.GRPC.Service, service_fun, [request, nil])
  end

  @doc """
  Helper to produce messages via HTTP API.
  """
  def produce_via_http(topic, records, opts \\ []) do
    partition = Keyword.get(opts, :partition, 0)
    acks = Keyword.get(opts, :acks, "leader")

    http_request(:post, "/v1/topics/#{topic}/records", %{
      "partition" => partition,
      "records" => records,
      "acks" => acks
    })
  end

  @doc """
  Helper to consume messages via HTTP API.
  """
  def consume_via_http(topic, opts \\ []) do
    partition = Keyword.get(opts, :partition, 0)
    group = Keyword.get(opts, :group, "")
    # earliest by default
    offset = Keyword.get(opts, :offset, "-2")
    max_bytes = Keyword.get(opts, :max_bytes, 1_048_576)
    auto_commit = Keyword.get(opts, :auto_commit, false)

    params = %{
      "partition" => partition,
      "group" => group,
      "offset" => to_string(offset),
      "max_bytes" => to_string(max_bytes),
      "auto_commit" => to_string(auto_commit)
    }

    http_request(:get, "/v1/topics/#{topic}/records", params)
  end

  @doc """
  Helper to create topic via HTTP API.
  """
  def create_topic_via_http(name, partitions \\ 1) do
    http_request(:post, "/v1/topics", %{
      "name" => name,
      "partitions" => partitions
    })
  end

  @doc """
  Helper to list topics via HTTP API.
  """
  def list_topics_via_http do
    http_request(:get, "/v1/topics")
  end

  @doc """
  Helper to commit offsets via HTTP API.
  """
  def commit_offsets_via_http(group, offsets) do
    http_request(:post, "/v1/offsets/commit", %{
      "group" => group,
      "offsets" => offsets
    })
  end
end
