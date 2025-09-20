defmodule KafkaesqueClient.Admin do
  @moduledoc """
  Administrative client for managing Kafkaesque topics and metadata.

  Provides operations for creating topics, listing topics, and querying offsets.
  """
  use GenServer
  require Logger

  alias KafkaesqueClient.Connection.Pool

  defstruct [:pool, :config, :metrics]

  @type t :: %__MODULE__{
          pool: atom() | pid(),
          config: map(),
          metrics: map()
        }

  # Client API

  @doc """
  Starts an admin client with the given configuration.

  ## Options

  - `:bootstrap_servers` - List of Kafkaesque servers (required)
  - `:request_timeout_ms` - Request timeout in milliseconds (default: 30000)
  """
  def start_link(config) when is_map(config) do
    GenServer.start_link(__MODULE__, config)
  end

  @doc """
  Creates a new topic with the specified configuration.

  ## Options

  - `:partitions` - Number of partitions (default: 1)
  - `:batch_size` - Max records per batch (default: 500)
  - `:batch_timeout` - Batch timeout in milliseconds (default: 5000)
  - `:min_demand` - Min demand for GenStage (default: 5)
  - `:max_demand` - Max demand for GenStage (default: 500)

  ## Examples

      Admin.create_topic(admin, "my-topic", partitions: 3)

      Admin.create_topic(admin, "high-throughput",
        partitions: 5,
        batch_size: 1000,
        batch_timeout: 1000,
        min_demand: 10,
        max_demand: 1000
      )
  """
  @spec create_topic(GenServer.server(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def create_topic(admin, topic_name, opts \\ []) do
    GenServer.call(admin, {:create_topic, topic_name, opts})
  end

  @doc """
  Lists all topics in the cluster.
  """
  @spec list_topics(GenServer.server()) :: {:ok, [map()]} | {:error, term()}
  def list_topics(admin) do
    GenServer.call(admin, :list_topics)
  end

  @doc """
  Gets the earliest and latest offsets for a topic partition.

  ## Examples

      Admin.get_offsets(admin, "my-topic", 0)
  """
  @spec get_offsets(GenServer.server(), String.t(), integer()) ::
          {:ok, %{earliest: integer(), latest: integer()}} | {:error, term()}
  def get_offsets(admin, topic, partition \\ 0) do
    GenServer.call(admin, {:get_offsets, topic, partition})
  end

  @doc """
  Deletes a topic (if supported by the server).

  Note: Topic deletion may not be immediately supported in the MVP.
  """
  @spec delete_topic(GenServer.server(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def delete_topic(admin, topic_name) do
    GenServer.call(admin, {:delete_topic, topic_name})
  end

  @doc """
  Describes a topic, returning its configuration and metadata.
  """
  @spec describe_topic(GenServer.server(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def describe_topic(admin, topic_name) do
    GenServer.call(admin, {:describe_topic, topic_name})
  end

  @doc """
  Lists consumer groups (if supported by the server).
  """
  @spec list_consumer_groups(GenServer.server()) ::
          {:ok, [String.t()]} | {:error, :not_supported}
  def list_consumer_groups(admin) do
    GenServer.call(admin, :list_consumer_groups)
  end

  @doc """
  Describes a consumer group, including its members and offsets.
  """
  @spec describe_consumer_group(GenServer.server(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def describe_consumer_group(admin, group_id) do
    GenServer.call(admin, {:describe_consumer_group, group_id})
  end

  @doc """
  Returns metrics about the admin client.
  """
  @spec metrics(GenServer.server()) :: map()
  def metrics(admin) do
    GenServer.call(admin, :metrics)
  end

  @doc """
  Closes the admin client.
  """
  @spec close(GenServer.server()) :: :ok
  def close(admin) do
    GenServer.call(admin, :close)
  end

  # Server Callbacks

  @impl true
  def init(config) do
    state = %__MODULE__{
      pool: Pool,
      config: config,
      metrics: %{
        operations: 0,
        errors: 0,
        topics_created: 0,
        topics_deleted: 0
      }
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:create_topic, topic_name, opts}, _from, state) do
    partitions = Keyword.get(opts, :partitions, 1)
    batch_size = Keyword.get(opts, :batch_size, 0)
    batch_timeout_ms = Keyword.get(opts, :batch_timeout, 0)
    min_demand = Keyword.get(opts, :min_demand, 0)
    max_demand = Keyword.get(opts, :max_demand, 0)

    request = %Kafkaesque.CreateTopicRequest{
      name: topic_name,
      partitions: partitions,
      batch_size: batch_size,
      batch_timeout_ms: batch_timeout_ms,
      min_demand: min_demand,
      max_demand: max_demand
    }

    result =
      case Pool.execute(state.pool, {:create_topic, request}) do
        {:ok, topic} ->
          {:ok,
           %{
             name: topic.name,
             partitions: topic.partitions
           }}

        {:error, reason} ->
          {:error, reason}
      end

    new_metrics =
      case result do
        {:ok, _} ->
          %{
            state.metrics
            | operations: state.metrics.operations + 1,
              topics_created: state.metrics.topics_created + 1
          }

        {:error, _} ->
          %{
            state.metrics
            | operations: state.metrics.operations + 1,
              errors: state.metrics.errors + 1
          }
      end

    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call(:list_topics, _from, state) do
    request = %Kafkaesque.ListTopicsRequest{}

    result =
      case Pool.execute(state.pool, {:list_topics, request}) do
        {:ok, response} ->
          topics =
            Enum.map(response.topics, fn topic ->
              %{
                name: topic.name,
                partitions: topic.partitions
              }
            end)

          {:ok, topics}

        {:error, reason} ->
          {:error, reason}
      end

    new_metrics = update_metrics(state.metrics, result)
    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call({:get_offsets, topic, partition}, _from, state) do
    request = %Kafkaesque.GetOffsetsRequest{
      topic: topic,
      partition: partition
    }

    result =
      case Pool.execute(state.pool, {:get_offsets, request}) do
        {:ok, response} ->
          {:ok,
           %{
             earliest: response.earliest,
             latest: response.latest
           }}

        {:error, reason} ->
          {:error, reason}
      end

    new_metrics = update_metrics(state.metrics, result)
    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call({:delete_topic, _topic_name}, _from, state) do
    # Topic deletion not supported in MVP
    result = {:error, :not_supported}
    new_metrics = update_metrics(state.metrics, result)
    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call({:describe_topic, topic_name}, _from, state) do
    # Get basic topic info by listing topics and filtering
    request = %Kafkaesque.ListTopicsRequest{}

    result =
      case Pool.execute(state.pool, {:list_topics, request}) do
        {:ok, response} ->
          case Enum.find(response.topics, fn t -> t.name == topic_name end) do
            nil ->
              {:error, :topic_not_found}

            topic ->
              # Get offsets for each partition
              partition_info =
                for partition <- 0..(topic.partitions - 1) do
                  offsets_req = %Kafkaesque.GetOffsetsRequest{
                    topic: topic_name,
                    partition: partition
                  }

                  offsets =
                    case Pool.execute(state.pool, {:get_offsets, offsets_req}) do
                      {:ok, resp} -> %{earliest: resp.earliest, latest: resp.latest}
                      _ -> %{earliest: 0, latest: 0}
                    end

                  %{
                    partition: partition,
                    earliest_offset: offsets.earliest,
                    latest_offset: offsets.latest,
                    message_count: offsets.latest - offsets.earliest
                  }
                end

              {:ok,
               %{
                 name: topic.name,
                 partitions: topic.partitions,
                 partition_info: partition_info
               }}
          end

        {:error, reason} ->
          {:error, reason}
      end

    new_metrics = update_metrics(state.metrics, result)
    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call(:list_consumer_groups, _from, state) do
    # Consumer group listing not supported in MVP
    result = {:error, :not_supported}
    new_metrics = update_metrics(state.metrics, result)
    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call({:describe_consumer_group, _group_id}, _from, state) do
    # Consumer group description not supported in MVP
    result = {:error, :not_supported}
    new_metrics = update_metrics(state.metrics, result)
    {:reply, result, %{state | metrics: new_metrics}}
  end

  def handle_call(:metrics, _from, state) do
    {:reply, state.metrics, state}
  end

  def handle_call(:close, _from, state) do
    {:stop, :normal, :ok, state}
  end

  # Private Functions

  defp update_metrics(metrics, {:ok, _}) do
    %{metrics | operations: metrics.operations + 1}
  end

  defp update_metrics(metrics, {:error, _}) do
    %{metrics | operations: metrics.operations + 1, errors: metrics.errors + 1}
  end
end
