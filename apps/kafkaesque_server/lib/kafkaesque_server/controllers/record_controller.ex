defmodule KafkaesqueServer.RecordController do
  use Phoenix.Controller, formats: [:json, :html]

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Partition.Router
  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.LogReader
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  require Logger

  def produce(conn, %{"topic" => topic} = params) do
    partition_param = params |> Map.get("partition") |> normalize_partition_param()
    records = Map.get(params, "records", [])
    acks = Map.get(params, "acks", "leader")

    if records == [] do
      conn
      |> put_status(:bad_request)
      |> json(%{error: "Records are required"})
    else
      case TopicSupervisor.get_topic_info(topic) do
        {:error, :topic_not_found} ->
          conn
          |> put_status(:not_found)
          |> json(%{error: "Topic #{topic} does not exist"})

        {:ok, topic_info} ->
          routing_key = records |> first_json_key() |> coerce_routing_key()

          case resolve_produce_partition(topic_info, partition_param, routing_key) do
            {:ok, partition} ->
              Logger.info("REST: Producing #{length(records)} records to #{topic}/#{partition}")

              messages =
                Enum.map(records, fn record ->
                  %{
                    key: Map.get(record, "key", <<>>),
                    value: Map.get(record, "value", <<>>),
                    headers: Map.get(record, "headers", []) |> convert_headers(),
                    timestamp_ms: Map.get(record, "timestamp_ms", System.system_time(:millisecond))
                  }
                end)

              acks_atom = if acks == "none", do: :none, else: :leader

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

            {:error, message} ->
              conn
              |> put_status(:bad_request)
              |> json(%{error: message})
          end
      end
    end
  end

  def consume(conn, %{"topic" => topic} = params) do
    partition_param = params |> Map.get("partition") |> normalize_partition_param()
    group = Map.get(params, "group", "")
    offset = Map.get(params, "offset", "-1") |> parse_offset()
    format = Map.get(params, "format", "json")

    # Validate and parse parameters
    case validate_consume_params(params) do
      {:ok, validated_params} ->
        max_bytes = validated_params.max_bytes
        max_wait_ms = validated_params.max_wait_ms
        auto_commit = validated_params.auto_commit

        case TopicSupervisor.get_topic_info(topic) do
          {:error, :topic_not_found} ->
            conn
            |> put_status(:not_found)
            |> json(%{error: "Topic #{topic} does not exist"})

          {:ok, topic_info} ->
            case resolve_consume_partition(topic_info, partition_param) do
              {:ok, partition} ->
                Logger.info("REST: Consuming from #{topic}/#{partition} for group #{group}")

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
                  try do
                    case LogReader.consume(topic, partition, group, starting_offset, max_bytes, max_wait_ms) do
                      {:ok, records} ->
                        if auto_commit and group != "" and records != [] do
                          last_offset = starting_offset + length(records) - 1
                          DetsOffset.commit(topic, partition, group, last_offset)
                        end

                        {_, high_watermark} =
                          case SingleFile.get_offsets(topic, partition) do
                            {:ok, %{latest: hw}} -> {0, hw}
                            _ -> {0, starting_offset}
                          end

                        actual_base_offset =
                          case starting_offset do
                            :earliest -> 0
                            :latest -> if records == [], do: high_watermark, else: List.first(records)[:offset] || 0
                            n when is_integer(n) -> n
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

              {:error, message} ->
                conn
                |> put_status(:bad_request)
                |> json(%{error: message})
            end
        end

      {:error, error_msg} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: error_msg})
    end
  end

  # Private functions

  defp normalize_partition_param(nil), do: -1
  defp normalize_partition_param(partition) when is_integer(partition), do: partition

  defp normalize_partition_param(partition) when is_binary(partition) do
    case Integer.parse(partition) do
      {value, ""} -> value
      _ -> -1
    end
  end

  defp normalize_partition_param(_), do: -1

  defp resolve_produce_partition(topic_info, partition, routing_key) when partition < 0 do
    {:ok, Router.route(routing_key, topic_info.partitions)}
  end

  defp resolve_produce_partition(topic_info, partition, _routing_key) do
    if partition in topic_info.partition_ids do
      {:ok, partition}
    else
      {:error,
       "Invalid partition #{partition} for topic #{topic_info.name} (available: #{Enum.join(topic_info.partition_ids, ", ")})"}
    end
  end

  defp resolve_consume_partition(topic_info, partition) when partition < 0 do
    case topic_info.partition_ids do
      [first | _] -> {:ok, first}
      _ -> {:error, "Topic #{topic_info.name} has no partitions"}
    end
  end

  defp resolve_consume_partition(topic_info, partition) do
    if partition in topic_info.partition_ids do
      {:ok, partition}
    else
      {:error,
       "Invalid partition #{partition} for topic #{topic_info.name} (available: #{Enum.join(topic_info.partition_ids, ", ")})"}
    end
  end

  defp first_json_key([]), do: nil
  defp first_json_key(nil), do: nil
  defp first_json_key([record | _]) when is_map(record), do: Map.get(record, "key")
  defp first_json_key(record) when is_map(record), do: Map.get(record, "key")
  defp first_json_key(_), do: nil

  defp coerce_routing_key(key) when is_binary(key) and byte_size(key) > 0, do: key
  defp coerce_routing_key(_), do: nil

  defp validate_consume_params(params) do
    max_bytes = Map.get(params, "max_bytes", "1048576") |> String.to_integer()
    max_wait_ms = Map.get(params, "max_wait_ms", "500") |> String.to_integer()
    auto_commit = Map.get(params, "auto_commit", "true") == "true"

    cond do
      max_bytes < 0 or max_bytes > 100_000_000 ->
        {:error, "Invalid max_bytes: must be between 0 and 100000000"}

      max_wait_ms < 0 or max_wait_ms > 60_000 ->
        {:error, "Invalid max_wait_ms: must be between 0 and 60000"}

      true ->
        {:ok, %{max_bytes: max_bytes, max_wait_ms: max_wait_ms, auto_commit: auto_commit}}
    end
  end

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
         auto_commit,
         start_time \\ nil
       ) do
    start_time = start_time || System.monotonic_time(:millisecond)
    max_duration = Application.get_env(:kafkaesque_server, :sse_max_duration_ms, 300_000)

    # Check if stream has been running too long
    if System.monotonic_time(:millisecond) - start_time > max_duration do
      chunk(conn, "event: close\ndata: {\"reason\": \"max_duration_exceeded\"}\n\n")
      conn
    else
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

            case chunk(conn, "data: #{event_data}\n\n") do
              {:ok, conn} -> conn
              {:error, _reason} ->
                # Client disconnected - exit the loop by returning conn
                throw({:client_disconnected, conn})
            end
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
            auto_commit,
            start_time
          )

        {:ok, []} ->
          # No records, send heartbeat and continue
          case chunk(conn, ":heartbeat\n\n") do
            {:ok, conn} ->
              Process.sleep(100)

              sse_consume_loop(
                conn,
                topic,
                partition,
                group,
                offset,
                max_bytes,
                max_wait_ms,
                auto_commit,
                start_time
              )

            {:error, _reason} ->
              # Client disconnected
              conn
          end

        {:error, reason} ->
          Logger.error("SSE consume error: #{inspect(reason)}")
          chunk(conn, "event: error\ndata: #{Jason.encode!(%{error: inspect(reason)})}\n\n")
          conn
      end
    end
  catch
    {:client_disconnected, conn} ->
      Logger.info("SSE client disconnected for #{topic}/#{partition}")
      conn
  end
end
