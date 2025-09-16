defmodule Kafkaesque.Pipeline.BatchingTest do
  use Kafkaesque.TestCase, async: false

  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  import Kafkaesque.TestHelpers, only: [wait_for_messages: 4]

  setup do
    # Ensure clean state before test
    ensure_clean_state()

    # Use a unique test directory in system temp
    test_dir = Path.join(System.tmp_dir!(), "kafkaesque_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(test_dir)
    Application.put_env(:kafkaesque_core, :data_dir, test_dir)
    Application.put_env(:kafkaesque_core, :offsets_dir, Path.join(test_dir, "offsets"))

    # Generate unique topic name for this test
    test_topic = unique_topic_name("batching_test")
    test_partition = 0

    # Create a fresh topic with custom batch settings
    {:ok, _} = TopicSupervisor.create_topic(test_topic, 1)

    # Give processes time to initialize
    Process.sleep(200)

    on_exit(fn ->
      if TopicSupervisor.topic_exists?(test_topic) do
        TopicSupervisor.delete_topic(test_topic)
      end
      File.rm_rf!(test_dir)
    end)

    {:ok, test_topic: test_topic, test_partition: test_partition}
  end

  describe "time-based batching" do
    test "messages are batched and written after timeout", %{test_topic: test_topic, test_partition: test_partition} do
      # Send a small number of messages (less than batch size)
      messages = Enum.map(1..3, fn i ->
        %{
          key: "timeout_key_#{i}",
          value: "timeout_value_#{i}",
          headers: []
        }
      end)

      # Record time before producing
      _start_time = System.monotonic_time(:millisecond)

      {:ok, _} = Producer.produce(test_topic, test_partition, messages)

      # Wait for messages to be written (timeout-based batch)
      assert :ok = wait_for_messages(test_topic, test_partition, 3, 2000)

      # Now messages should be written
      {:ok, after_timeout} = SingleFile.read(test_topic, test_partition, 0, 10_000)

      # Verify messages were eventually written
      assert length(after_timeout) == 3, "Expected 3 messages, got #{length(after_timeout)}"

      written_values = Enum.map(after_timeout, & &1.value)
      assert "timeout_value_1" in written_values
      assert "timeout_value_2" in written_values
      assert "timeout_value_3" in written_values
    end
  end

  describe "size-based batching" do
    test "full batch is written immediately", %{test_topic: test_topic, test_partition: test_partition} do
      batch_size = Application.get_env(:kafkaesque_core, :max_batch_size, 500)

      # Create exactly batch_size messages
      messages = Enum.map(1..batch_size, fn i ->
        %{
          key: "batch_key_#{i}",
          value: "batch_value_#{i}",
          headers: []
        }
      end)

      {:ok, _} = Producer.produce(test_topic, test_partition, messages)

      # Full batch should trigger immediate write
      assert :ok = wait_for_messages(test_topic, test_partition, batch_size, 2000)

      {:ok, stored} = SingleFile.read(test_topic, test_partition, 0, 100_000)

      # All messages should be written
      assert length(stored) == batch_size, "Expected #{batch_size} messages, got #{length(stored)}"
    end

    test "oversized batch is split correctly", %{test_topic: test_topic, test_partition: test_partition} do
      batch_size = Application.get_env(:kafkaesque_core, :max_batch_size, 500)

      # Create more than batch_size messages
      total_messages = batch_size + 50
      messages = Enum.map(1..total_messages, fn i ->
        %{
          key: "overflow_key_#{i}",
          value: "overflow_value_#{i}",
          headers: []
        }
      end)

      {:ok, _} = Producer.produce(test_topic, test_partition, messages)

      # Wait for all messages to be written (might be multiple batches)
      assert :ok = wait_for_messages(test_topic, test_partition, total_messages, 5000)

      {:ok, stored} = SingleFile.read(test_topic, test_partition, 0, 100_000)

      # All messages should eventually be written
      assert length(stored) == total_messages, "Expected #{total_messages} messages, got #{length(stored)}"
    end
  end

  describe "batch ordering" do
    test "messages maintain order within batches", %{test_topic: test_topic, test_partition: test_partition} do
      # Send messages with sequential identifiers
      messages = Enum.map(1..20, fn i ->
        %{
          key: "order_#{String.pad_leading(Integer.to_string(i), 3, "0")}",
          value: "value_#{i}",
          headers: [{"seq", Integer.to_string(i)}]
        }
      end)

      {:ok, _} = Producer.produce(test_topic, test_partition, messages)

      # Wait for messages to be written
      assert :ok = wait_for_messages(test_topic, test_partition, 20, 2000)

      {:ok, stored} = SingleFile.read(test_topic, test_partition, 0, 10_000)

      # Verify we have messages
      assert length(stored) > 0, "No messages were stored"

      # Verify order is maintained for the messages we got
      stored_keys = Enum.map(stored, & &1.key)
      expected_keys = Enum.map(1..length(stored), fn i ->
        "order_#{String.pad_leading(Integer.to_string(i), 3, "0")}"
      end)

      assert expected_keys == stored_keys, "Order not maintained"
    end
  end

  describe "concurrent batching" do
    test "multiple producers can write concurrently", %{test_topic: test_topic, test_partition: test_partition} do
      # Spawn multiple processes to produce messages
      tasks = Enum.map(1..5, fn producer_id ->
        Task.async(fn ->
          messages = Enum.map(1..10, fn msg_id ->
            %{
              key: "producer_#{producer_id}_msg_#{msg_id}",
              value: "concurrent_value_#{producer_id}_#{msg_id}",
              headers: [{"producer", Integer.to_string(producer_id)}]
            }
          end)

          Producer.produce(test_topic, test_partition, messages)
        end)
      end)

      # Wait for all tasks to complete
      results = Enum.map(tasks, &Task.await/1)

      # All should succeed
      Enum.each(results, fn result ->
        assert {:ok, _} = result
      end)

      # Wait for all messages from concurrent producers
      assert :ok = wait_for_messages(test_topic, test_partition, 50, 5000)

      # Read all messages
      {:ok, stored} = SingleFile.read(test_topic, test_partition, 0, 100_000)

      # Should have all 50 messages eventually
      assert length(stored) == 50

      # Verify all producers' messages are present
      for producer_id <- 1..5 do
        producer_messages = Enum.filter(stored, fn msg ->
          String.starts_with?(msg.key, "producer_#{producer_id}_")
        end)
        assert length(producer_messages) == 10
      end
    end
  end

  describe "batch telemetry" do
    @tag :telemetry
    test "batch writes emit telemetry events", %{test_topic: test_topic, test_partition: test_partition} do
      # Attach telemetry handler
      ref = make_ref()
      test_pid = self()
      handler_id = "test-handler-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:kafkaesque, :batching_consumer, :batch_written],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      # Produce enough messages for a full batch
      batch_size = Application.get_env(:kafkaesque_core, :max_batch_size, 500)
      messages = Enum.map(1..batch_size, fn i ->
        %{key: "telemetry_#{i}", value: "value_#{i}", headers: []}
      end)

      {:ok, _} = Producer.produce(test_topic, test_partition, messages)

      # Wait for telemetry event
      assert_receive {:telemetry, [:kafkaesque, :batching_consumer, :batch_written],
                      measurements, metadata}, 2000

      # Verify measurements
      assert measurements.count == batch_size
      assert measurements.duration_ms >= 0  # Can be 0 for very fast operations
      assert measurements.bytes > 0

      # Verify metadata
      assert metadata.topic == test_topic
      assert metadata.partition == test_partition

      # Cleanup
      :telemetry.detach(handler_id)
    end
  end
end
