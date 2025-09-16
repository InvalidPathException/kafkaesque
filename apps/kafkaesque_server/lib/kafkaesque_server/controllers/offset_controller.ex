defmodule KafkaesqueServer.OffsetController do
  use Phoenix.Controller, formats: [:json]

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Storage.SingleFile

  require Logger

  def commit(conn, %{"group" => group} = params) do
    offsets = Map.get(params, "offsets", [])

    if offsets == [] do
      conn
      |> put_status(:bad_request)
      |> json(%{error: "Offsets are required"})
    else
      Logger.info("REST: Committing offsets for group #{group}")

      # Commit each offset
      results =
        Enum.map(offsets, fn offset_data ->
          topic = Map.get(offset_data, "topic", "")
          partition = Map.get(offset_data, "partition", 0)
          offset = Map.get(offset_data, "offset", 0)

          case DetsOffset.commit(topic, partition, group, offset) do
            :ok ->
              %{
                topic: topic,
                partition: partition,
                offset: offset,
                status: "committed"
              }

            {:error, reason} ->
              %{
                topic: topic,
                partition: partition,
                offset: offset,
                status: "error",
                error: inspect(reason)
              }
          end
        end)

      # Check if any commits failed
      failed = Enum.filter(results, fn r -> r[:status] == "error" end)

      if failed == [] do
        json(conn, %{
          group: group,
          offsets: results,
          status: "success"
        })
      else
        conn
        |> put_status(:partial_content)
        |> json(%{
          group: group,
          offsets: results,
          status: "partial",
          message: "Some offsets failed to commit"
        })
      end
    end
  end

  def get(conn, %{"topic" => topic} = params) do
    partition = Map.get(params, "partition", 0)

    Logger.info("REST: Getting offsets for #{topic}/#{partition}")

    case SingleFile.get_offsets(topic, partition) do
      {:ok, %{earliest: earliest, latest: latest}} ->
        json(conn, %{
          topic: topic,
          partition: partition,
          earliest: earliest,
          latest: latest
        })

      {:error, reason} ->
        conn
        |> put_status(:internal_server_error)
        |> json(%{error: "Failed to get offsets: #{inspect(reason)}"})
    end
  end
end
