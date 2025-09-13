defmodule Kafkaesque.TestCase do
  @moduledoc """
  Shared test helpers and setup for Kafkaesque tests.
  """

  use ExUnit.CaseTemplate

  alias Kafkaesque.Pipeline.MessageBuffer
  alias Kafkaesque.Pipeline.ProduceBroadway
  alias Kafkaesque.Storage.SingleFile

  using do
    quote do
      import Kafkaesque.TestCase

      @test_dir "./test_#{:erlang.unique_integer([:positive])}"
    end
  end

  setup do
    # Start required registries and services
    ensure_registry_started()
    # Only start PubSub if the application isn't already managing it
    unless Application.started_applications() |> Enum.any?(fn {app, _, _} -> app == :kafkaesque_core end) do
      ensure_pubsub_started()
    end
    ensure_telemetry_started()

    :ok
  end

  @doc """
  Ensures the Registry is started.
  """
  def ensure_registry_started do
    case Registry.start_link(keys: :unique, name: Kafkaesque.TopicRegistry) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end
  end

  @doc """
  Ensures PubSub is started.
  """
  def ensure_pubsub_started do
    # Check if PubSub is already running
    case Process.whereis(Kafkaesque.PubSub) do
      nil ->
        # Not running, start it
        children = [
          {Phoenix.PubSub, name: Kafkaesque.PubSub}
        ]

        case Supervisor.start_link(children, strategy: :one_for_one, name: KafkaesqueTestPubSub) do
          {:ok, _} -> :ok
          {:error, {:already_started, _}} -> :ok
          {:error, {:shutdown, {:failed_to_start_child, Phoenix.PubSub, {:already_started, _}}}} -> :ok
        end

      _pid ->
        # Already running
        :ok
    end
  end

  @doc """
  Ensures Telemetry is started.
  """
  def ensure_telemetry_started do
    case Kafkaesque.Telemetry.start_link([]) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end
  end

  @doc """
  Sets up a test directory and ensures cleanup on exit.
  """
  def setup_test_dir(dir \\ nil) do
    test_dir = dir || "./test_#{:erlang.unique_integer([:positive])}"
    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)

    on_exit(fn ->
      File.rm_rf!(test_dir)
    end)

    test_dir
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
        {:ok, pid}

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

    # Start message buffer
    {:ok, buffer_pid} =
      MessageBuffer.start_link(
        topic: topic,
        partition: partition,
        max_queue_size: Keyword.get(opts, :max_queue_size, 100)
      )

    # Start Broadway pipeline
    {:ok, broadway_pid} =
      ProduceBroadway.start_link(
        topic: topic,
        partition: partition,
        batch_size: Keyword.get(opts, :batch_size, 10),
        batch_timeout: Keyword.get(opts, :batch_timeout, 1)
      )

    %{
      storage: storage_pid,
      buffer: buffer_pid,
      broadway: broadway_pid,
      data_dir: data_dir
    }
  end

  @doc """
  Cleans up a test topic and its processes.
  """
  def cleanup_test_topic(topic, partition) do
    # Stop Broadway
    ProduceBroadway.drain_and_stop(topic, partition)

    # Close storage
    try do
      SingleFile.close(topic, partition)
    catch
      :exit, _ -> :ok
    end

    # Unregister from registry
    Registry.unregister(Kafkaesque.TopicRegistry, {:storage, topic, partition})
    Registry.unregister(Kafkaesque.TopicRegistry, {:message_buffer, topic, partition})

    :ok
  end
end
