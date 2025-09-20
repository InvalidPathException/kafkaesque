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

      # Extract batch configuration from request
      opts = []
      opts = maybe_add_opt(opts, :batch_size, params["batch_size"])
      opts = maybe_add_opt(opts, :batch_timeout, params["batch_timeout_ms"])
      opts = maybe_add_opt(opts, :min_demand, params["min_demand"])
      opts = maybe_add_opt(opts, :max_demand, params["max_demand"])

      case TopicSupervisor.create_topic(name, partitions, opts) do
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

  defp maybe_add_opt(opts, key, value) when is_integer(value) and value > 0 do
    [{key, value} | opts]
  end
  defp maybe_add_opt(opts, _key, _value), do: opts

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
