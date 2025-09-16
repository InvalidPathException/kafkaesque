defmodule KafkaesqueServer.RecordController do
  use Phoenix.Controller, formats: [:json, :html]

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.LogReader

  require Logger

  def produce(conn, %{"topic" => topic} = params) do
    partition = Map.get(params, "partition", 0)
    records = Map.get(params, "records", [])
    acks = Map.get(params, "acks", "leader")

    if records == [] do
      conn
      |> put_status(:bad_request)
      |> json(%{error: "Records are required"})
    else
      Logger.info("REST: Producing #{length(records)} records to #{topic}/#{partition}")

      # Convert JSON records to internal format
      messages =
        Enum.map(records, fn record ->
          %{
            key: Map.get(record, "key", <<>>),
            value: Map.get(record, "value", <<>>),
            headers: Map.get(record, "headers", []) |> convert_headers(),
            timestamp_ms: Map.get(record, "timestamp_ms", System.system_time(:millisecond))
          }
        end)

      # Determine acks level
      acks_atom =
        case acks do
          "none" -> :none
          _ -> :leader
        end

      case Producer.produce(topic, partition, messages, acks: acks_atom) do
        {:ok, result} ->
          json(conn, %{
            topic: topic,
            partition: partition,
            base_offset: Map.get(result, :base_offset, 0),
            count: result.count,
            status: "success"
          })

        {:error, :backpressure} ->
          conn
          |> put_status(:too_many_requests)
          |> json(%{error: "Producer queue is full, please retry"})

        {:error, reason} ->
          conn
          |> put_status(:internal_server_error)
          |> json(%{error: "Failed to produce: #{inspect(reason)}"})
      end
    end
  end

  def consume(conn, %{"topic" => topic} = params) do
    partition = Map.get(params, "partition", 0)
    group = Map.get(params, "group", "")
    offset = Map.get(params, "offset", "-1") |> parse_offset()
    max_bytes = Map.get(params, "max_bytes", "1048576") |> String.to_integer()
    max_wait_ms = Map.get(params, "max_wait_ms", "500") |> String.to_integer()
    auto_commit = Map.get(params, "auto_commit", "true") == "true"
    format = Map.get(params, "format", "json")

    Logger.info("REST: Consuming from #{topic}/#{partition} for group #{group}")

    # Get starting offset
    starting_offset =
      if group != "" do
        case DetsOffset.fetch(topic, partition, group) do
          {:ok, committed_offset} -> committed_offset + 1
          {:error, :not_found} -> offset
          _ -> offset
        end
      else
        offset
      end

    if format == "sse" do
      # Server-Sent Events for streaming
      conn
      |> put_resp_header("content-type", "text/event-stream")
      |> put_resp_header("cache-control", "no-cache")
      |> send_chunked(200)
      |> sse_consume_loop(
        topic,
        partition,
        group,
        starting_offset,
        max_bytes,
        max_wait_ms,
        auto_commit
      )
    else
      # Single fetch for JSON response
      try do
        case LogReader.consume(topic, partition, group, starting_offset, max_bytes, max_wait_ms) do
          {:ok, records} ->
            # Auto-commit if enabled
            if auto_commit and group != "" and records != [] do
              last_offset = starting_offset + length(records) - 1
              DetsOffset.commit(topic, partition, group, last_offset)
            end

            # Get high watermark
            {_, high_watermark} =
              case SingleFile.get_offsets(topic, partition) do
                {:ok, %{latest: hw}} -> {0, hw}
                _ -> {0, starting_offset}
              end

            # Determine actual base offset for response
            actual_base_offset =
              case starting_offset do
                :earliest ->
                  0

                :latest ->
                  if records == [], do: high_watermark, else: List.first(records)[:offset] || 0

                n when is_integer(n) ->
                  n
              end

            json(conn, %{
              topic: topic,
              partition: partition,
              high_watermark: high_watermark,
              base_offset: actual_base_offset,
              records: Enum.map(records, &format_record/1)
            })

          {:error, reason} ->
            conn
            |> put_status(:internal_server_error)
            |> json(%{error: "Failed to consume: #{inspect(reason)}"})
        end
      catch
        :exit, {:noproc, _} ->
          conn
          |> put_status(:internal_server_error)
          |> json(%{error: "Failed to consume: topic or partition does not exist"})
      end
    end
  end

  # Private functions

  defp convert_headers(headers) when is_list(headers) do
    Enum.map(headers, fn
      %{"key" => k, "value" => v} -> {k, v}
      {k, v} -> {k, v}
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp convert_headers(_), do: []

  defp parse_offset("-1"), do: :latest
  defp parse_offset("-2"), do: :earliest

  defp parse_offset(offset) when is_binary(offset) do
    case Integer.parse(offset) do
      {num, ""} when num >= 0 -> num
      _ -> :latest
    end
  end

  defp parse_offset(offset) when is_integer(offset) and offset >= 0, do: offset
  defp parse_offset(_), do: :latest

  defp format_record(record) do
    %{
      key: record[:key] || "",
      value: record[:value] || "",
      headers: format_headers(record[:headers] || []),
      timestamp_ms: record[:timestamp_ms] || 0,
      offset: record[:offset] || 0
    }
  end

  defp format_headers(headers) do
    Enum.map(headers, fn {k, v} ->
      %{key: k, value: v}
    end)
  end

  defp sse_consume_loop(
         conn,
         topic,
         partition,
         group,
         offset,
         max_bytes,
         max_wait_ms,
         auto_commit
       ) do
    case LogReader.consume(topic, partition, group, offset, max_bytes, max_wait_ms) do
      {:ok, records} when records != [] ->
        # Send records as SSE events
        Enum.each(records, fn record ->
          event_data =
            Jason.encode!(%{
              topic: topic,
              partition: partition,
              record: format_record(record)
            })

          chunk(conn, "data: #{event_data}\n\n")
        end)

        # Auto-commit if enabled
        last_offset = offset + length(records) - 1

        if auto_commit and group != "" do
          DetsOffset.commit(topic, partition, group, last_offset)
        end

        # Continue consuming
        sse_consume_loop(
          conn,
          topic,
          partition,
          group,
          last_offset + 1,
          max_bytes,
          max_wait_ms,
          auto_commit
        )

      {:ok, []} ->
        # No records, send heartbeat and continue
        chunk(conn, ":heartbeat\n\n")
        Process.sleep(100)

        sse_consume_loop(
          conn,
          topic,
          partition,
          group,
          offset,
          max_bytes,
          max_wait_ms,
          auto_commit
        )

      {:error, reason} ->
        Logger.error("SSE consume error: #{inspect(reason)}")
        chunk(conn, "event: error\ndata: #{Jason.encode!(%{error: inspect(reason)})}\n\n")
        conn
    end
  end
end
