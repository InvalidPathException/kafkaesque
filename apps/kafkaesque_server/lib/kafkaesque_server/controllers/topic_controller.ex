defmodule KafkaesqueServer.TopicController do
  use Phoenix.Controller, formats: [:json]

  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  require Logger

  def create(conn, params) do
    name = Map.get(params, "name", "")
    partitions = Map.get(params, "partitions", 1)

    if name == "" do
      conn
      |> put_status(:bad_request)
      |> json(%{error: "Topic name is required"})
    else
      Logger.info("REST: Creating topic #{name} with #{partitions} partitions")

      case TopicSupervisor.create_topic(name, partitions) do
        {:ok, info} ->
          conn
          |> put_status(:created)
          |> json(%{
            topic: info.topic,
            partitions: info.partitions,
            status: "created"
          })

        {:error, reason} ->
          conn
          |> put_status(:internal_server_error)
          |> json(%{error: "Failed to create topic: #{inspect(reason)}"})
      end
    end
  end

  def index(conn, _params) do
    Logger.info("REST: Listing topics")

    topics = TopicSupervisor.list_topics()

    topic_list =
      Enum.map(topics, fn topic ->
        case topic do
          {name, partitions} when is_binary(name) and is_integer(partitions) ->
            %{
              name: name,
              partitions: partitions
            }

          %{name: name, partitions: partitions} ->
            %{
              name: name,
              partitions: partitions
            }

          _ ->
            nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    json(conn, %{topics: topic_list})
  end
end
