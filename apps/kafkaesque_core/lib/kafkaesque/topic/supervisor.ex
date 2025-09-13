defmodule Kafkaesque.Topic.Supervisor do
  @moduledoc """
  DynamicSupervisor for topic partitions.
  Manages supervision tree per topic-partition.
  """

  use DynamicSupervisor
  require Logger

  alias Broadway
  alias Kafkaesque.Telemetry

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Creates a new topic with the specified number of partitions.
  """
  def create_topic(name, partitions \\ 1) when is_binary(name) and partitions > 0 do
    Logger.info("Creating topic '#{name}' with #{partitions} partitions")

    # Start supervisor for each partition
    results =
      Enum.map(0..(partitions - 1), fn partition ->
        start_partition(name, partition)
      end)

    # Check if all partitions started successfully
    case Enum.find(results, &match?({:error, _}, &1)) do
      nil ->
        # Emit telemetry
        Telemetry.topic_created(name, partitions)
        {:ok, %{topic: name, partitions: partitions}}

      {:error, reason} ->
        # Clean up any successfully started partitions
        Enum.each(results, fn
          {:ok, pid} -> DynamicSupervisor.terminate_child(__MODULE__, pid)
          _ -> :ok
        end)

        {:error, reason}
    end
  end

  @doc """
  Deletes a topic and all its partitions.
  """
  def delete_topic(name) when is_binary(name) do
    Logger.info("Deleting topic '#{name}'")

    # Find all Registry entries for this topic (including MessageBuffer, Broadway, etc.)
    all_keys =
      Registry.select(Kafkaesque.TopicRegistry, [
        {{:"$1", :_, :_}, [], [:"$1"]}
      ])

    # Filter for keys related to this topic
    topic_keys =
      Enum.filter(all_keys, fn
        {:storage, ^name, _partition} -> true
        {:message_buffer, ^name, _partition} -> true
        {:partition_sup, ^name, _partition} -> true
        {:log_reader, ^name, _partition} -> true
        {:retention_controller, ^name, _partition} -> true
        _ -> false
      end)

    # Stop and unregister all processes
    Enum.each(topic_keys, fn key ->
      case Registry.lookup(Kafkaesque.TopicRegistry, key) do
        [{pid, _}] ->
          try do
            if Process.alive?(pid) do
              Process.exit(pid, :shutdown)
            end
          catch
            _, _ -> :ok
          end

          Registry.unregister(Kafkaesque.TopicRegistry, key)

        _ ->
          :ok
      end
    end)

    # Also stop Broadway which uses atom names
    # Find all partitions for this topic
    partitions =
      Enum.flat_map(topic_keys, fn
        {:storage, ^name, partition} -> [partition]
        {:partition_sup, ^name, partition} -> [partition]
        _ -> []
      end)
      |> Enum.uniq()

    # Stop Broadway for each partition
    Enum.each(partitions, fn partition ->
      broadway_name = String.to_atom("broadway_#{name}_#{partition}")

      try do
        Broadway.stop(broadway_name, :normal, 5000)
      catch
        _, _ -> :ok
      end
    end)

    # Find and stop all partition supervisors for this topic
    partition_data =
      Registry.select(Kafkaesque.TopicRegistry, [
        {{{:partition_sup, :"$1", :"$2"}, :"$3", :_}, [{:==, :"$1", name}], [{{:"$2", :"$3"}}]}
      ])

    Logger.debug("Terminating #{length(partition_data)} partition supervisors for topic #{name}")

    Enum.each(partition_data, fn {_partition, pid} ->
      try do
        # Terminate the supervisor (this should clean up Registry entries automatically)
        if Process.alive?(pid) do
          DynamicSupervisor.terminate_child(__MODULE__, pid)

          # Wait a bit for cleanup
          Process.sleep(50)
        end
      catch
        _, _ -> :ok
      end
    end)

    # Double-check and force cleanup any remaining entries
    Registry.select(Kafkaesque.TopicRegistry, [
      {{{:partition_sup, :"$1", :"$2"}, :"$3", :_}, [{:==, :"$1", name}],
       [{{:partition_sup, :"$1", :"$2"}}]}
    ])
    |> Enum.each(fn key ->
      Registry.unregister_match(Kafkaesque.TopicRegistry, key, :_)
    end)

    # Emit telemetry
    Telemetry.topic_deleted(name)

    :ok
  end

  @doc """
  Lists all active topics and their partitions.
  """
  def list_topics do
    # Get all partition supervisor entries from Registry
    Registry.select(Kafkaesque.TopicRegistry, [
      {{{:partition_sup, :"$1", :"$2"}, :_, :_}, [], [{{:"$1", :"$2"}}]}
    ])
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {topic, partitions} ->
      %{
        name: topic,
        partitions: length(partitions),
        partition_ids: Enum.sort(partitions)
      }
    end)
  end

  @doc """
  Gets information about a specific topic.
  """
  def get_topic_info(name) do
    case find_topic_partitions(name) do
      [] ->
        {:error, :topic_not_found}

      partitions ->
        info = %{
          name: name,
          partitions: length(partitions),
          partition_ids: Enum.sort(partitions),
          status: :active
        }

        {:ok, info}
    end
  end

  @doc """
  Checks if a topic exists.
  """
  def topic_exists?(name) do
    length(find_topic_partitions(name)) > 0
  end

  defp start_partition(topic, partition) do
    child_spec = %{
      id: {topic, partition},
      start:
        {__MODULE__.PartitionSupervisor, :start_link, [[topic: topic, partition: partition]]},
      type: :supervisor,
      restart: :permanent
    }

    case DynamicSupervisor.start_child(__MODULE__, child_spec) do
      {:ok, pid} ->
        # The supervisor registers itself via the via_tuple with key {:partition_sup, topic, partition}
        {:ok, pid}

      error ->
        Logger.error("Failed to start partition #{topic}/#{partition}: #{inspect(error)}")
        error
    end
  end

  defp find_topic_partitions(topic) do
    Registry.select(Kafkaesque.TopicRegistry, [
      {{{:partition_sup, :"$1", :"$2"}, :_, :_}, [{:==, :"$1", topic}], [:"$2"]}
    ])
  end
end

defmodule Kafkaesque.Topic.Supervisor.PartitionSupervisor do
  @moduledoc """
  Supervisor for a single topic partition.
  Manages all processes related to one partition.
  """

  use Supervisor
  require Logger

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    Supervisor.start_link(__MODULE__, opts, name: via_tuple(topic, partition))
  end

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    children = [
      # Storage layer
      {Kafkaesque.Storage.SingleFile,
       [
         topic: topic,
         partition: partition,
         data_dir: Application.get_env(:kafkaesque_core, :data_dir, "./data")
       ]},

      # Message buffer for queueing
      {Kafkaesque.Pipeline.MessageBuffer,
       [
         topic: topic,
         partition: partition,
         max_queue_size: Application.get_env(:kafkaesque_core, :max_queue_size, 10_000)
       ]},

      # Broadway pipeline for batching and processing
      {Kafkaesque.Pipeline.ProduceBroadway,
       [
         topic: topic,
         partition: partition,
         batch_size: Application.get_env(:kafkaesque_core, :max_batch_size, 500),
         batch_timeout: Application.get_env(:kafkaesque_core, :batch_timeout, 5),
         processor_concurrency: Application.get_env(:kafkaesque_core, :processor_concurrency, 10)
       ]}

      # TODO:
      # Log reader for consuming
      # {Kafkaesque.Topic.LogReader,
      #  [
      #    topic: topic,
      #    partition: partition
      #  ]},

      # Offset storage
      # {Kafkaesque.Offsets.DetsOffset,
      #  [
      #    topic: topic,
      #    partition: partition,
      #    offsets_dir: Application.get_env(:kafkaesque_core, :offsets_dir, "./offsets")
      #  ]},

      # Retention controller
      # {Kafkaesque.Topic.RetentionController,
      #  [
      #    topic: topic,
      #    partition: partition,
      #    retention_hours: Application.get_env(:kafkaesque_core, :retention_hours, 168)
      #  ]}
    ]

    Logger.info("Starting partition supervisor for #{topic}/#{partition}")

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:partition_sup, topic, partition}}}
  end
end

defmodule Kafkaesque.Topic.LogReader do
  @moduledoc """
  GenServer for reading from log files.
  Demand-driven with max_bytes/max_wait_ms support.
  """

  use GenServer
  require Logger

  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Telemetry

  defstruct [
    :topic,
    :partition,
    :consumers
  ]

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    GenServer.start_link(__MODULE__, opts, name: via_tuple(topic, partition))
  end

  def consume(topic, partition, consumer_group, offset, max_bytes, max_wait_ms) do
    GenServer.call(
      via_tuple(topic, partition),
      {:consume, consumer_group, offset, max_bytes, max_wait_ms},
      # Add buffer to timeout
      max_wait_ms + 5000
    )
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      topic: opts[:topic],
      partition: opts[:partition],
      consumers: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:consume, group, offset, max_bytes, max_wait_ms}, from, state) do
    # Register consumer
    consumer_id = {group, from}

    new_state =
      put_in(state.consumers[consumer_id], %{
        group: group,
        offset: offset,
        max_bytes: max_bytes,
        started_at: System.monotonic_time(:millisecond)
      })

    # Try to read immediately
    case try_read(state.topic, state.partition, offset, max_bytes) do
      {:ok, records} when records != [] ->
        # Got data, return immediately
        Telemetry.consumer_joined(group, inspect(from))
        {:reply, {:ok, records}, new_state}

      _ ->
        # No data yet, set up long polling
        if max_wait_ms > 0 do
          Process.send_after(self(), {:timeout_consumer, consumer_id}, max_wait_ms)
          {:noreply, new_state}
        else
          {:reply, {:ok, []}, new_state}
        end
    end
  end

  @impl true
  def handle_info({:timeout_consumer, consumer_id}, state) do
    case Map.pop(state.consumers, consumer_id) do
      {nil, _} ->
        # Consumer already handled
        {:noreply, state}

      {consumer, new_consumers} ->
        # Try one more read
        {_group, from} = consumer_id

        case try_read(state.topic, state.partition, consumer.offset, consumer.max_bytes) do
          {:ok, records} ->
            GenServer.reply(from, {:ok, records})

          {:error, reason} ->
            GenServer.reply(from, {:error, reason})
        end

        {:noreply, %{state | consumers: new_consumers}}
    end
  end

  @impl true
  def handle_info({:new_data, _offset}, state) do
    # New data available, check waiting consumers
    {ready_consumers, waiting_consumers} =
      Enum.split_with(state.consumers, fn {_id, consumer} ->
        case try_read(state.topic, state.partition, consumer.offset, consumer.max_bytes) do
          {:ok, records} when records != [] -> true
          _ -> false
        end
      end)

    # Reply to ready consumers
    Enum.each(ready_consumers, fn {{_group, from}, consumer} ->
      {:ok, records} = try_read(state.topic, state.partition, consumer.offset, consumer.max_bytes)
      GenServer.reply(from, {:ok, records})
    end)

    # Keep waiting consumers
    new_consumers = Enum.into(waiting_consumers, %{})
    {:noreply, %{state | consumers: new_consumers}}
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:log_reader, topic, partition}}}
  end

  defp try_read(topic, partition, offset, max_bytes) do
    start_time = System.monotonic_time()

    result = SingleFile.read(topic, partition, offset, max_bytes)

    # Emit telemetry
    duration_ms =
      System.convert_time_unit(
        System.monotonic_time() - start_time,
        :native,
        :millisecond
      )

    case result do
      {:ok, records} ->
        bytes =
          Enum.reduce(records, 0, fn r, acc ->
            acc + byte_size(r[:value] || <<>>)
          end)

        Telemetry.storage_read(topic, partition, bytes, duration_ms)

        Enum.each(records, fn record ->
          Telemetry.message_consumed(%{
            topic: topic,
            partition: partition,
            bytes: byte_size(record[:value] || <<>>),
            latency_ms: duration_ms
          })
        end)

      _ ->
        :ok
    end

    result
  end
end

defmodule Kafkaesque.Offsets.DetsOffset do
  @moduledoc """
  DETS-based offset storage for consumer groups.
  """

  use GenServer
  require Logger

  @table_name :consumer_offsets

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    offsets_dir = Keyword.get(opts, :offsets_dir, "./offsets")
    File.mkdir_p!(offsets_dir)

    file_path = Path.join(offsets_dir, "offsets.dets")

    {:ok, table} =
      :dets.open_file(@table_name,
        file: String.to_charlist(file_path),
        type: :set
      )

    {:ok, %{table: table, topic: opts[:topic], partition: opts[:partition]}}
  end

  def commit(topic, partition, group, offset) do
    key = {topic, partition, group}
    :dets.insert(@table_name, {key, offset})
    Kafkaesque.Telemetry.offset_committed(group, topic, partition, offset)
    :ok
  end

  def fetch(topic, partition, group) do
    key = {topic, partition, group}

    case :dets.lookup(@table_name, key) do
      [{^key, offset}] -> {:ok, offset}
      # Start from beginning
      [] -> {:ok, 0}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table_name)
  end
end
