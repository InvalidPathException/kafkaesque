defmodule Kafkaesque.TestCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Kafkaesque.Pipeline.BatchingConsumer
  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile

  using do
    quote do
      import Kafkaesque.TestCase
    end
  end

  setup do
    # The application is started in test_helper.exs, so we don't need to start services here
    # They should already be running
    :ok
  end

  @doc """
  Sets up a test directory and ensures cleanup on exit.
  """
  def setup_test_dir(dir \\ nil) do
    test_dir = if dir do
      Path.join(System.tmp_dir!(), dir)
    else
      Path.join(System.tmp_dir!(), "kafkaesque_test_#{:erlang.unique_integer([:positive])}")
    end

    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)

    on_exit(fn ->
      File.rm_rf!(test_dir)
    end)

    test_dir
  end

  @doc """
  Generates a unique topic name for testing to avoid conflicts.
  """
  def unique_topic_name(base_name \\ "test_topic") do
    "#{base_name}_#{System.unique_integer([:positive])}"
  end

  @doc """
  Ensures the system is in a clean state by removing all topics and waiting for cleanup.
  This should be called in setup to ensure no interference from previous tests.
  """
  def ensure_clean_state(timeout \\ 5000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    # Delete all existing topics
    if Process.whereis(Kafkaesque.Topic.Supervisor) do
      topics = Kafkaesque.Topic.Supervisor.list_topics()

      Enum.each(topics, fn topic ->
        # Force flush any pending batches before deletion
        try do
          flush_topic_batches(topic.name)
        catch
          _kind, _reason -> :ok
        end

        Kafkaesque.Topic.Supervisor.delete_topic(topic.name)
      end)

      # Wait a bit for deletion to complete
      if length(topics) > 0 do
        Process.sleep(200)
      end

      # Wait until no topics exist
      wait_for_clean_topics(deadline)

      # Wait until Registry is clean
      wait_for_clean_registry(deadline)

      # Clean up any orphaned processes
      kill_orphaned_processes()

      # Small final delay to ensure everything settles
      Process.sleep(50)
    end

    :ok
  end

  defp flush_topic_batches(topic_name) do
    # Try to flush batches for all partitions
    case Kafkaesque.Topic.Supervisor.get_topic_info(topic_name) do
      {:ok, topic_info} ->
        Enum.each(0..(topic_info.partitions - 1), fn partition ->
          # Send a flush signal to the batching consumer
          case Registry.lookup(Kafkaesque.TopicRegistry, {:batching_consumer, topic_name, partition}) do
            [{pid, _}] when is_pid(pid) ->
              try do
                GenServer.call(pid, :flush_batch, 500)
              catch
                :exit, _ -> :ok
              end
            _ -> :ok
          end
        end)
      _ ->
        :ok
    end
  end

  defp kill_orphaned_processes do
    # Kill any lingering topic-related processes
    Registry.select(Kafkaesque.TopicRegistry, [{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.each(fn {_key, pid} ->
      if is_pid(pid) and Process.alive?(pid) do
        Process.exit(pid, :kill)
      end
    end)
  end

  defp wait_for_clean_topics(deadline) do
    if System.monotonic_time(:millisecond) < deadline do
      case Kafkaesque.Topic.Supervisor.list_topics() do
        [] ->
          :ok
        _topics ->
          # Just wait a bit and check again, don't try to delete
          # The initial delete_topic calls should have started the cleanup
          Process.sleep(50)
          wait_for_clean_topics(deadline)
      end
    else
      # Silently continue if topics remain after timeout
      # This is usually fine as tests use unique names anyway
      _remaining = Kafkaesque.Topic.Supervisor.list_topics()
      :ok
    end
  end

  defp wait_for_clean_registry(deadline) do
    if System.monotonic_time(:millisecond) < deadline do
      # Check for any topic-related entries in Registry
      entries = Registry.select(Kafkaesque.TopicRegistry, [{{:"$1", :"$2", :"$3"}, [], [:"$1"]}])

      topic_entries = Enum.filter(entries, fn
        {:storage, _, _} -> true
        {:producer, _, _} -> true
        {:batching_consumer, _, _} -> true
        {:partition_sup, _, _} -> true
        {:log_reader, _, _} -> true
        {:retention_controller, _, _} -> true
        {:offset_store, _, _} -> true
        _ -> false
      end)

      if length(topic_entries) > 0 do
        Process.sleep(50)
        wait_for_clean_registry(deadline)
      else
        :ok
      end
    else
      :ok
    end
  end

  @doc """
  Waits for a process to be registered in the Registry.
  """
  def wait_for_registration(key, timeout \\ 1000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_for_registration(key, deadline)
  end

  defp do_wait_for_registration(key, deadline) do
    case Registry.lookup(Kafkaesque.TopicRegistry, key) do
      [{pid, _}] when is_pid(pid) ->
        if Process.alive?(pid) do
          {:ok, pid}
        else
          # Process registered but not alive, wait more
          if System.monotonic_time(:millisecond) < deadline do
            Process.sleep(10)
            do_wait_for_registration(key, deadline)
          else
            {:error, :timeout}
          end
        end

      _ ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(10)
          do_wait_for_registration(key, deadline)
        else
          {:error, :timeout}
        end
    end
  end

  @doc """
  Retries a function until it succeeds or times out.
  """
  def retry_until_success(fun, timeout \\ 2000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_retry_until_success(fun, deadline)
  end

  defp do_retry_until_success(fun, deadline) do
    case fun.() do
      {:ok, result} ->
        {:ok, result}

      true ->
        {:ok, true}

      false ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(50)
          do_retry_until_success(fun, deadline)
        else
          {:error, :timeout}
        end

      {:error, _} = error ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(50)
          do_retry_until_success(fun, deadline)
        else
          error
        end
    end
  end

  @doc """
  Waits for a process to terminate.
  """
  def wait_for_termination(pid, timeout \\ 1000) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} ->
        :ok
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        {:error, :timeout}
    end
  end

  @doc """
  Creates test records with sequential keys and values.
  """
  def create_test_records(count, prefix \\ "test") do
    Enum.map(1..count, fn i ->
      %{
        key: "#{prefix}_key_#{i}",
        value: "#{prefix}_value_#{i}",
        headers: [{"header_#{i}", "value_#{i}"}],
        timestamp_ms: System.system_time(:millisecond)
      }
    end)
  end

  @doc """
  Starts a topic with all required processes and waits for them to be ready.
  """
  def start_test_topic(topic, partition, opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, setup_test_dir())

    # Start storage
    {:ok, storage_pid} =
      SingleFile.start_link(
        topic: topic,
        partition: partition,
        data_dir: data_dir
      )

    # Start GenStage producer
    {:ok, producer_pid} =
      Producer.start_link(
        topic: topic,
        partition: partition,
        max_queue_size: Keyword.get(opts, :max_queue_size, 100)
      )

    # Start GenStage consumer
    {:ok, consumer_pid} =
      BatchingConsumer.start_link(
        topic: topic,
        partition: partition,
        batch_size: Keyword.get(opts, :batch_size, 10),
        batch_timeout: Keyword.get(opts, :batch_timeout, 1000)
      )

    %{
      storage: storage_pid,
      producer: producer_pid,
      consumer: consumer_pid,
      data_dir: data_dir
    }
  end

  @doc """
  Cleans up a test topic and its processes.
  """
  def cleanup_test_topic(topic, partition) do
    # Stop GenStage processes
    with [{pid, _}] <- Registry.lookup(Kafkaesque.TopicRegistry, {:producer, topic, partition}) do
      GenStage.stop(pid, :normal, 5000)
    end

    with [{pid, _}] <- Registry.lookup(Kafkaesque.TopicRegistry, {:batching_consumer, topic, partition}) do
      GenStage.stop(pid, :normal, 5000)
    end

    # Close storage
    try do
      SingleFile.close(topic, partition)
    catch
      :exit, _ -> :ok
    end

    # Unregister from registry
    Registry.unregister(Kafkaesque.TopicRegistry, {:storage, topic, partition})
    Registry.unregister(Kafkaesque.TopicRegistry, {:producer, topic, partition})
    Registry.unregister(Kafkaesque.TopicRegistry, {:batching_consumer, topic, partition})

    :ok
  end
end
