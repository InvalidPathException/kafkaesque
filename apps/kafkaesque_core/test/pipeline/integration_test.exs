defmodule Kafkaesque.Pipeline.IntegrationTest do
  use Kafkaesque.TestCase, async: false

  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  setup do
    # Ensure clean state before test
    ensure_clean_state()

    # Use a unique test directory in system temp
    test_dir = Path.join(System.tmp_dir!(), "kafkaesque_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(test_dir)
    Application.put_env(:kafkaesque_core, :data_dir, test_dir)
    Application.put_env(:kafkaesque_core, :offsets_dir, Path.join(test_dir, "offsets"))

    # Generate unique topic name for this test
    test_topic = unique_topic_name("integration_test")
    test_partition = 0

    # Create a fresh topic
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

  describe "end-to-end message flow" do
    test "messages flow from producer through consumer to storage", %{test_topic: test_topic, test_partition: test_partition} do
      # Produce messages
      messages = Enum.map(1..10, fn i ->
        %{
          key: "key_#{i}",
          value: "value_#{i}",
          headers: [{"header", "test"}]
        }
      end)

      # Send messages to producer
      {:ok, result} = Producer.produce(test_topic, test_partition, messages)
      assert result.count == 10

      # Wait for messages to flow through pipeline
      # The batching consumer has a timeout of up to 5 seconds
      batch_timeout = Application.get_env(:kafkaesque_core, :batch_timeout, 5) * 1000
      Process.sleep(batch_timeout + 500)

      # Read messages from storage
      {:ok, stored_messages} = SingleFile.read(test_topic, test_partition, 0, 10_000)

      # Verify all messages were stored
      assert length(stored_messages) == 10

      # Verify message content
      Enum.zip(messages, stored_messages)
      |> Enum.each(fn {original, stored} ->
        assert stored.key == original.key
        assert stored.value == original.value
        assert stored.headers == original.headers
      end)
    end

    test "backpressure is applied when queue is full", %{test_topic: test_topic, test_partition: test_partition} do
      # Get current max queue size
      _max_queue_size = Application.get_env(:kafkaesque_core, :max_queue_size, 10_000)

      # Create tasks to bombard the producer with messages concurrently
      # This should overwhelm the queue faster than the consumer can drain
      tasks = for task_num <- 1..20 do
        Task.async(fn ->
          messages = Enum.map(1..1000, fn i ->
            %{
              key: "task_#{task_num}_msg_#{i}",
              value: String.duplicate("x", 100),
              headers: []
            }
          end)

          Producer.produce(test_topic, test_partition, messages)
        end)
      end

      # Wait for tasks and collect results
      results = Enum.map(tasks, &Task.await(&1, 10_000))

      # At least one should have gotten backpressure
      backpressure_count = Enum.count(results, fn result ->
        match?({:error, :backpressure}, result)
      end)

      assert backpressure_count > 0, "Expected at least one backpressure error, but got none out of #{length(results)} attempts"
    end

    test "messages are persisted correctly with CRC validation", %{test_topic: test_topic, test_partition: test_partition} do
      # Produce a message with specific content
      message = %{
        key: "crc_test_key",
        value: "crc_test_value",
        headers: [{"crc_header", "test_value"}],
        timestamp_ms: 1_234_567_890
      }

      {:ok, _} = Producer.produce(test_topic, test_partition, [message])

      # Wait for batch timeout to ensure persistence
      batch_timeout = Application.get_env(:kafkaesque_core, :batch_timeout, 5) * 1000
      Process.sleep(batch_timeout + 1000)

      # Read back and verify
      {:ok, stored_messages} = SingleFile.read(test_topic, test_partition, 0, 10_000)
      assert length(stored_messages) > 0, "No messages were stored"
      stored = hd(stored_messages)

      assert stored.key == message.key
      assert stored.value == message.value
      assert stored.headers == message.headers
      assert stored.timestamp_ms == message.timestamp_ms
    end

    test "multiple partitions work independently", %{test_topic: _test_topic, test_partition: _test_partition} do
      # Create topic with multiple partitions
      multi_topic = unique_topic_name("multi_partition")
      {:ok, _} = TopicSupervisor.create_topic(multi_topic, 3)
      Process.sleep(100)

      # Produce to different partitions
      msg_p0 = [%{key: "p0", value: "partition_0", headers: []}]
      msg_p1 = [%{key: "p1", value: "partition_1", headers: []}]
      msg_p2 = [%{key: "p2", value: "partition_2", headers: []}]

      {:ok, _} = Producer.produce(multi_topic, 0, msg_p0)
      {:ok, _} = Producer.produce(multi_topic, 1, msg_p1)
      {:ok, _} = Producer.produce(multi_topic, 2, msg_p2)

      # Wait for batch timeout
      batch_timeout = Application.get_env(:kafkaesque_core, :batch_timeout, 5) * 1000
      Process.sleep(batch_timeout + 1000)

      # Verify each partition has its own message
      {:ok, msgs_p0} = SingleFile.read(multi_topic, 0, 0, 10_000)
      {:ok, msgs_p1} = SingleFile.read(multi_topic, 1, 0, 10_000)
      {:ok, msgs_p2} = SingleFile.read(multi_topic, 2, 0, 10_000)

      assert length(msgs_p0) > 0, "No messages in partition 0"
      assert length(msgs_p1) > 0, "No messages in partition 1"
      assert length(msgs_p2) > 0, "No messages in partition 2"

      stored_p0 = hd(msgs_p0)
      stored_p1 = hd(msgs_p1)
      stored_p2 = hd(msgs_p2)

      assert stored_p0.value == "partition_0"
      assert stored_p1.value == "partition_1"
      assert stored_p2.value == "partition_2"

      # Cleanup
      if TopicSupervisor.topic_exists?(multi_topic) do
        TopicSupervisor.delete_topic(multi_topic)
      end
    end
  end

  describe "producer stats" do
    test "producer reports queue statistics", %{test_topic: test_topic, test_partition: test_partition} do
      # Send some messages
      messages = Enum.map(1..5, fn i ->
        %{key: "stat_key_#{i}", value: "stat_value_#{i}", headers: []}
      end)

      {:ok, _} = Producer.produce(test_topic, test_partition, messages)

      # Get stats
      stats = Producer.get_stats(test_topic, test_partition)

      assert stats.topic == test_topic
      assert stats.partition == test_partition
      assert stats.queue_size >= 0
      assert stats.max_queue_size > 0
      assert stats.utilization >= 0.0
      assert stats.dropped_messages >= 0
    end
  end
end
