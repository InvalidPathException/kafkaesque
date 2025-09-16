defmodule Kafkaesque.Test.Helpers do
  @moduledoc """
  Consolidated test helpers for all Kafkaesque tests.
  """

  import ExUnit.Assertions
  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Test.{Config, Factory}
  alias Kafkaesque.Topic.LogReader
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  @doc """
  Set up a clean test environment with proper isolation.
  """
  def setup_test do
    # Get unique test directories
    {data_dir, offsets_dir} = Config.ensure_test_dirs()

    # Update application env with test-specific paths
    Application.put_env(:kafkaesque_core, :data_dir, data_dir)
    Application.put_env(:kafkaesque_core, :offsets_dir, offsets_dir)

    # Clean ETS tables
    clean_ets_tables()

    # Return test context
    %{
      data_dir: data_dir,
      offsets_dir: offsets_dir
    }
  end

  @doc """
  Clean up after test.
  """
  def cleanup_test(context) do
    # Clean directories
    if context[:data_dir], do: File.rm_rf!(context.data_dir)
    if context[:offsets_dir], do: File.rm_rf!(context.offsets_dir)

    # Clean ETS tables
    clean_ets_tables()

    :ok
  end

  @doc """
  Create a test topic with automatic cleanup and retry logic.
  """
  def create_test_topic(opts \\ []) do
    name = Keyword.get(opts, :name, Factory.unique_topic_name())
    partitions = Keyword.get(opts, :partitions, 1)

    create_topic_with_retry(name, partitions, 3)
  end

  defp create_topic_with_retry(name, partitions, retries) do
    case TopicSupervisor.create_topic(name, partitions) do
      {:ok, info} ->
        # Wait for all components to be ready
        wait_for_topic_ready(name, partitions)
        {:ok, name, info}

      {:error, {:already_started, _}} when retries > 0 ->
        # Topic exists, try to clean up and retry
        TopicSupervisor.delete_topic(name)
        # Exponential backoff
        Process.sleep(100 * (4 - retries))
        create_topic_with_retry(name, partitions, retries - 1)

      {:error, :already_exists} when retries > 0 ->
        # Topic exists, try to clean up and retry
        TopicSupervisor.delete_topic(name)
        # Exponential backoff
        Process.sleep(100 * (4 - retries))
        create_topic_with_retry(name, partitions, retries - 1)

      error ->
        error
    end
  end

  @doc """
  Wait for a topic to be fully initialized.
  """
  def wait_for_topic_ready(topic, partitions, timeout \\ 5000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Enum.each(0..(partitions - 1), fn partition ->
      # Wait for all key components to be registered
      wait_for_component(topic, partition, :storage, deadline)
      wait_for_component(topic, partition, :producer, deadline)
      wait_for_component(topic, partition, :batching_consumer, deadline)
      wait_for_component(topic, partition, :log_reader, deadline)
      # Small delay to ensure everything is fully initialized
      Process.sleep(50)
    end)

    :ok
  end

  defp wait_for_component(topic, partition, component, deadline) do
    # All components use {:component, topic, partition} format
    key = {component, topic, partition}

    case wait_until(
           fn ->
             case Registry.lookup(Kafkaesque.TopicRegistry, key) do
               [{pid, _}] when is_pid(pid) -> Process.alive?(pid)
               _ -> false
             end
           end,
           deadline - System.monotonic_time(:millisecond)
         ) do
      :ok -> :ok
      _ -> raise "Timeout waiting for #{component} #{topic}/#{partition}"
    end
  end

  @doc """
  Produce messages and wait for them to be written.
  """
  def produce_and_wait(topic, partition, messages, timeout \\ 5000) do
    # Get initial offset to know where we'll start reading
    {:ok, initial} = SingleFile.get_offsets(topic, partition)

    # Produce messages
    case Producer.produce(topic, partition, messages) do
      {:ok, result} ->
        # Wait for messages to be actually written to storage (not just queued)
        # We need to verify we can read the messages from storage
        wait_until(
          fn ->
            # Check the latest offset has been updated to include our messages
            case SingleFile.get_offsets(topic, partition) do
              {:ok, offsets} ->
                # Check if the offset has advanced by the number of messages we produced
                messages_written = offsets.latest - initial.latest

                if messages_written >= length(messages) do
                  # Double-check by actually reading the messages
                  case SingleFile.read(
                         topic,
                         partition,
                         initial.latest,
                         1_048_576
                       ) do
                    {:ok, records} -> length(records) >= length(messages)
                    _ -> false
                  end
                else
                  false
                end

              _ ->
                false
            end
          end,
          timeout
        )

        # Small additional delay to ensure any index updates complete
        Process.sleep(50)

        {:ok, result}

      error ->
        error
    end
  end

  @doc """
  Consume messages directly from storage.
  """
  def consume_messages(topic, partition, offset \\ 0, max_messages \\ 100) do
    # Rough estimate
    max_bytes = max_messages * 1000

    case LogReader.consume(topic, partition, "", offset, max_bytes, 100) do
      {:ok, messages} -> {:ok, messages}
      error -> error
    end
  end

  @doc """
  Assert that a topic exists with expected partitions.
  """
  def assert_topic_exists(name, expected_partitions) do
    case TopicSupervisor.get_topic_info(name) do
      {:ok, info} ->
        # Field is 'name', not 'topic'
        assert info.name == name
        assert info.partitions == expected_partitions
        :ok

      _ ->
        flunk("Topic #{name} does not exist")
    end
  end

  @doc """
  Clean a specific topic and wait for cleanup.
  """
  def delete_test_topic(name, timeout \\ 5000) do
    case TopicSupervisor.delete_topic(name) do
      :ok ->
        wait_until(
          fn ->
            case TopicSupervisor.get_topic_info(name) do
              # Correct error atom
              {:error, :topic_not_found} -> true
              _ -> false
            end
          end,
          timeout
        )

      _ ->
        :ok
    end
  end

  @doc """
  Generate test messages.
  """
  defdelegate generate_messages(count, opts \\ []), to: Factory

  @doc """
  Wait until a condition is true or timeout.
  """
  def wait_until(fun, timeout \\ 2000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        do_wait_until(fun, deadline)
      else
        {:error, :timeout}
      end
    end
  end

  @doc """
  Clean ETS tables used by the system.
  """
  def clean_ets_tables do
    # Clean telemetry table
    case :ets.whereis(:kafkaesque_telemetry) do
      :undefined -> :ok
      _ -> :ets.delete_all_objects(:kafkaesque_telemetry)
    end

    # Clean other known ETS tables
    Enum.each([:kafkaesque_offsets, :kafkaesque_index], fn table ->
      case :ets.whereis(table) do
        :undefined -> :ok
        _ -> :ets.delete_all_objects(table)
      end
    end)
  end

  @doc """
  Force flush batches for a topic.
  """
  def flush_topic_batches(topic, partition) do
    # Send flush signal to batching consumer
    case Registry.lookup(Kafkaesque.TopicRegistry, {:consumer, topic, partition}) do
      [{pid, _}] when is_pid(pid) ->
        try do
          GenServer.call(pid, :flush_batch, 500)
        catch
          :exit, _ -> :ok
        end

      _ ->
        :ok
    end
  end
end
