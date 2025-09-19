defmodule Kafkaesque.Topic.Supervisor do
  @moduledoc """
  DynamicSupervisor for topic partitions.
  Manages supervision tree per topic-partition.
  """

  use DynamicSupervisor
  require Logger

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
  def create_topic(name, partitions \\ 1, opts \\ []) when is_binary(name) and partitions > 0 do
    Logger.debug("Creating topic '#{name}' with #{partitions} partitions")

    # Start supervisor for each partition
    results =
      Enum.map(0..(partitions - 1), fn partition ->
        start_partition(name, partition, opts)
      end)

    # Check if all partitions started successfully
    case Enum.find(results, &match?({:error, _}, &1)) do
      nil ->
        # Emit telemetry
        Telemetry.topic_created(name, partitions)
        Logger.debug("Topic created: #{name} with #{partitions} partitions")
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
    Logger.debug("Deleting topic '#{name}'")

    # Find partition supervisors via Registry
    partition_supervisors =
      Registry.select(Kafkaesque.TopicRegistry, [
        {{{:partition_sup, :"$1", :"$2"}, :"$3", :_}, [{:==, :"$1", name}], [{{:"$2", :"$3"}}]}
      ])

    Logger.debug(
      "Terminating #{length(partition_supervisors)} partition supervisors for topic #{name}"
    )

    # Terminate each partition supervisor (this will cascade to all child processes)
    partition_supervisors
    |> Enum.each(fn {_partition, pid} ->
      if is_pid(pid) and Process.alive?(pid) do
        # Use synchronous termination via DynamicSupervisor
        case DynamicSupervisor.terminate_child(__MODULE__, pid) do
          :ok ->
            # Wait for process to fully terminate
            ref = Process.monitor(pid)

            receive do
              {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
            after
              1000 ->
                Process.demonitor(ref, [:flush])
                Logger.warning("Timeout waiting for partition supervisor to terminate")
            end

          error ->
            Logger.warning("Failed to terminate partition supervisor: #{inspect(error)}")
        end
      end
    end)

    # Clean up any stale Registry entries for this topic
    Registry.select(Kafkaesque.TopicRegistry, [
      {{:"$1", :_, :_}, [], [:"$1"]}
    ])
    |> Enum.filter(fn
      {:storage, ^name, _} -> true
      {:producer, ^name, _} -> true
      {:batching_consumer, ^name, _} -> true
      {:partition_sup, ^name, _} -> true
      {:log_reader, ^name, _} -> true
      {:retention_controller, ^name, _} -> true
      {:offset_store, ^name, _} -> true
      _ -> false
    end)
    |> Enum.each(fn key ->
      Registry.unregister_match(Kafkaesque.TopicRegistry, key, :_)
    end)

    # Small delay to ensure full cleanup
    Process.sleep(10)

    # Emit telemetry
    Telemetry.topic_deleted(name)

    Logger.debug("Topic deleted: #{name}")

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
    # Check if any partition supervisors are registered for this topic
    Registry.select(Kafkaesque.TopicRegistry, [
      {{{:partition_sup, :"$1", :_}, :_, :_}, [{:==, :"$1", name}], [true]}
    ])
    |> Enum.any?()
  end

  defp start_partition(topic, partition, opts) do
    child_spec = %{
      id: {topic, partition},
      start:
        {__MODULE__.PartitionSupervisor, :start_link, [Keyword.merge([topic: topic, partition: partition], opts)]},
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

      # GenStage Producer for message queueing
      {Kafkaesque.Pipeline.Producer,
       [
         topic: topic,
         partition: partition,
         max_queue_size: Application.get_env(:kafkaesque_core, :max_queue_size, 10_000)
       ]},

      # GenStage BatchingConsumer for writing to storage
      {Kafkaesque.Pipeline.BatchingConsumer,
       [
         topic: topic,
         partition: partition,
         batch_size: Keyword.get(opts, :batch_size, Application.get_env(:kafkaesque_core, :max_batch_size, 500)),
         batch_timeout: Keyword.get(opts, :batch_timeout, Application.get_env(:kafkaesque_core, :batch_timeout, 5000)),
         min_demand: Keyword.get(opts, :min_demand, Application.get_env(:kafkaesque_core, :min_demand, 5)),
         max_demand: Keyword.get(opts, :max_demand, Application.get_env(:kafkaesque_core, :max_batch_size, 500))
       ]},

      # Log reader for consuming
      {Kafkaesque.Topic.LogReader,
       [
         topic: topic,
         partition: partition
       ]},

      # Offset storage
      {Kafkaesque.Offsets.DetsOffset,
       [
         topic: topic,
         partition: partition,
         offsets_dir: Application.get_env(:kafkaesque_core, :offsets_dir, "./offsets")
       ]},

      # Retention controller
      {Kafkaesque.Topic.RetentionController,
       [
         topic: topic,
         partition: partition,
         retention_ms:
           Application.get_env(:kafkaesque_core, :retention_hours, 168) * 60 * 60 * 1000,
         storage_path: Application.get_env(:kafkaesque_core, :data_dir, "./data")
       ]}
    ]

    Logger.debug("Starting partition supervisor for #{topic}/#{partition}")

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
  alias Kafkaesque.Utils.Retry

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

          {:error, :offset_out_of_range} ->
            # Offset is beyond current data, return empty list
            GenServer.reply(from, {:ok, []})

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

    # Use retry logic for transient storage failures
    result =
      Retry.retry_with_backoff(
        fn ->
          SingleFile.read(topic, partition, offset, max_bytes)
        end,
        max_retries: 2,
        initial_delay: 50,
        retry_on: [:enoent, :eagain, :timeout]
      )

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

        {:ok, records}

      error ->
        error
    end
  end
end

defmodule Kafkaesque.Offsets.DetsOffset do
  @moduledoc """
  DETS-based offset storage for consumer groups.
  """

  use GenServer
  require Logger

  def start_link(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    GenServer.start_link(__MODULE__, opts, name: via_tuple(topic, partition))
  end

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    offsets_dir = Keyword.get(opts, :offsets_dir, "./offsets")

    File.mkdir_p!(offsets_dir)

    # Use topic-partition specific table name
    table_name = :"offsets_#{topic}_#{partition}"
    file_path = Path.join(offsets_dir, "#{topic}_#{partition}_offsets.dets")

    {:ok, table} =
      :dets.open_file(table_name,
        file: String.to_charlist(file_path),
        type: :set
      )

    {:ok, %{table: table, table_name: table_name, topic: topic, partition: partition}}
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {Kafkaesque.TopicRegistry, {:offset_store, topic, partition}}}
  end

  def commit(topic, partition, group, offset) do
    GenServer.call(via_tuple(topic, partition), {:commit, group, offset})
  end

  def fetch(topic, partition, group) do
    GenServer.call(via_tuple(topic, partition), {:fetch, group})
  end

  @impl true
  def handle_call({:commit, group, offset}, _from, state) do
    key = {state.topic, state.partition, group}
    :dets.insert(state.table_name, {key, offset})
    Kafkaesque.Telemetry.offset_committed(group, state.topic, state.partition, offset)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:fetch, group}, _from, state) do
    key = {state.topic, state.partition, group}

    result =
      case :dets.lookup(state.table_name, key) do
        [{^key, offset}] -> {:ok, offset}
        # No committed offset for this group
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table_name)
  end
end
