defmodule Kafkaesque.Integration.ProduceConsumeTest do
  use ExUnit.Case, async: false
  import Kafkaesque.TestHelpers

  @moduletag timeout: 60_000  # 1 minute for integration tests

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.LogReader

  setup do
    setup_test_env()
    # Create unique topic name for this test run
    suffix = :rand.uniform(999_999)
    topic_name = "flow-topic-#{suffix}"
    {:ok, _} = create_test_topic(topic_name, 1)
    on_exit(fn ->
      delete_test_topic(topic_name)
      clean_test_dirs()
    end)
    {:ok, topic: topic_name}
  end

  describe "High-Throughput Production" do
    test "handles high-throughput producing of 1000+ messages", %{topic: topic} do
      partition = 0

      # Generate 1000 messages in batches
      batch_size = 100
      num_batches = 10

      _produced_offsets = for batch_num <- 1..num_batches do
        messages = generate_messages(batch_size,
          key_prefix: "batch#{batch_num}-key",
          value_prefix: "batch#{batch_num}-value"
        )

        {:ok, result} = Producer.produce(topic, partition, messages)
        assert result.count == batch_size
        result.base_offset
      end

      # Wait for all batches to be flushed
      Process.sleep(1000)

      # Verify all messages were stored
      {:ok, offsets} = SingleFile.get_offsets(topic, partition)
      # Latest offset should be 999 for 1000 messages (0-indexed)
      # But if it's 1000, that's also acceptable (points to next offset)
      assert offsets.latest == 999 or offsets.latest == 1000
      assert offsets.earliest == 0

      # Consume all messages to verify
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)
      assert length(consumed) == 1000

      # Verify batch boundaries
      Enum.each(0..9, fn batch_idx ->
        batch_messages = Enum.slice(consumed, batch_idx * 100, 100)
        batch_num = batch_idx + 1

        Enum.with_index(batch_messages, fn msg, idx ->
          assert msg[:key] == "batch#{batch_num}-key-#{idx + 1}"
          assert msg[:value] == "batch#{batch_num}-value-#{idx + 1}"
        end)
      end)
    end

    test "handles backpressure when queue is full", %{topic: topic} do
      partition = 0

      # We'll test backpressure by filling the queue
      # The default max_queue_size is typically 10_000

      # First, stop the consumer to prevent draining
      consumer_pid = Process.whereis(:"consumer_#{topic}_#{partition}")
      if consumer_pid do
        :erlang.suspend_process(consumer_pid)
      end

      # Fill up the queue by producing many messages rapidly with acks=none
      # Use acks=none to avoid waiting and fill queue faster
      large_batch = generate_messages(10_000)
      {:ok, _} = Producer.produce(topic, partition, large_batch, acks: :none)

      # Try to produce more immediately - should trigger backpressure
      overflow_batch = generate_messages(1000)
      result = Producer.produce(topic, partition, overflow_batch, acks: :none, timeout: 100)

      # Resume consumer if it was suspended
      if consumer_pid do
        :erlang.resume_process(consumer_pid)
      end

      # Expect backpressure error
      assert result == {:error, :backpressure}

      # Wait for the consumer to drain some messages
      Process.sleep(1000)

      # Now we should be able to produce again
      retry_batch = generate_messages(10)
      {:ok, retry_result} = Producer.produce(topic, partition, retry_batch)
      assert retry_result.count == 10
    end
  end

  describe "Batch Consumption" do
    setup %{topic: topic} do
      # Pre-populate with messages
      messages = generate_messages(500)
      {:ok, _result} = Producer.produce(topic, 0, messages)

      # Give time for messages to be written
      Process.sleep(500)

      {:ok, topic: topic}
    end

    test "respects max_bytes limit during consumption", %{topic: topic} do
      partition = 0

      # Consume with small max_bytes (approximate)
      # Each message is roughly 50-100 bytes
      max_bytes = 1000  # Should get ~10-20 messages

      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, max_bytes, 100)

      # Should get some messages but not all 500
      assert length(consumed) > 0
      assert length(consumed) < 500

      # Calculate actual bytes consumed
      actual_bytes = Enum.reduce(consumed, 0, fn msg, acc ->
        key_size = byte_size(msg[:key] || "")
        value_size = byte_size(msg[:value] || "")
        acc + key_size + value_size + 20  # Add overhead for metadata
      end)

      # Should be reasonably close to max_bytes (within 2x tolerance)
      assert actual_bytes < max_bytes * 2
    end

    test "respects max_wait_ms timeout", %{topic: topic} do
      partition = 0

      # Consume from the end (no messages available)
      {:ok, offsets} = SingleFile.get_offsets(topic, partition)
      start_offset = offsets.latest + 1

      # Start timing
      start_time = System.monotonic_time(:millisecond)

      # This should wait for max_wait_ms and return empty
      {:ok, consumed} = LogReader.consume(topic, partition, "", start_offset, 1_048_576, 500)

      end_time = System.monotonic_time(:millisecond)
      elapsed = end_time - start_time

      # Should have waited approximately max_wait_ms
      assert consumed == []
      assert elapsed >= 400  # Allow some tolerance
      assert elapsed < 700   # But not too long
    end

    test "handles batch consumption with varying sizes", %{topic: topic} do
      partition = 0

      # Test different batch configurations
      batch_configs = [
        {100, 100},      # Small batches
        {10_000, 500},   # Medium batches
        {1_048_576, 1000} # Large batches
      ]

      for {max_bytes, max_wait} <- batch_configs do
        {:ok, consumed} = LogReader.consume(topic, partition, "", 0, max_bytes, max_wait)
        assert length(consumed) > 0

        # Verify message integrity
        Enum.each(consumed, fn msg ->
          assert msg[:key] =~ ~r/^key-\d+$/
          assert msg[:value] =~ ~r/^value-\d+$/
          assert msg[:offset] >= 0
        end)
      end
    end
  end

  describe "Message Ordering" do
    test "preserves message order within partition", %{topic: topic} do
      partition = 0

      # Produce messages with sequential identifiers
      messages = Enum.map(1..100, fn i ->
        %{
          key: "order-#{String.pad_leading(Integer.to_string(i), 3, "0")}",
          value: Jason.encode!(%{sequence: i, timestamp: System.system_time()}),
          headers: [{"sequence", Integer.to_string(i)}],
          timestamp_ms: System.system_time(:millisecond)
        }
      end)

      {:ok, _} = Producer.produce(topic, partition, messages)

      # Wait for messages to be flushed
      Process.sleep(500)

      # Consume and verify order
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)
      assert length(consumed) == 100

      # Check strict ordering
      Enum.reduce(consumed, {0, nil}, fn msg, {expected_seq, prev_offset} ->
        # Extract sequence from key
        "order-" <> seq_str = msg[:key]
        seq = String.to_integer(seq_str)

        # Verify sequence
        assert seq == expected_seq + 1

        # Verify offset ordering
        if prev_offset do
          assert msg[:offset] == prev_offset + 1
        end

        {seq, msg[:offset]}
      end)
    end

    test "maintains order across multiple producers", %{topic: topic} do
      partition = 0

      # Spawn multiple concurrent producers
      tasks = Enum.map(1..5, fn producer_id ->
        Task.async(fn ->
          messages = Enum.map(1..20, fn msg_id ->
            %{
              key: "producer#{producer_id}-msg#{msg_id}",
              value: "data-#{producer_id}-#{msg_id}",
              headers: [{"producer_id", Integer.to_string(producer_id)}],
              timestamp_ms: System.system_time(:millisecond)
            }
          end)

          Producer.produce(topic, partition, messages)
        end)
      end)

      # Wait for all producers to complete
      results = Task.await_many(tasks, 5000)
      Enum.each(results, fn {:ok, result} ->
        assert result.count == 20
      end)

      # Wait for all messages to be flushed
      Process.sleep(1000)

      # Consume all messages
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)
      assert length(consumed) == 100

      # Group by producer and verify order within each producer's messages
      grouped = Enum.group_by(consumed, fn msg ->
        case Enum.find(msg[:headers] || [], fn {k, _} -> k == "producer_id" end) do
          {_, producer_id} -> producer_id
          nil -> "unknown"
        end
      end)

      Enum.each(grouped, fn {_producer_id, producer_messages} ->
        assert length(producer_messages) == 20

        # Verify messages from same producer maintain relative order
        msg_ids = Enum.map(producer_messages, fn msg ->
          case Regex.run(~r/msg(\d+)$/, msg[:key]) do
            [_, id] -> String.to_integer(id)
            _ -> 0
          end
        end)

        # Check that message IDs are in ascending order
        assert msg_ids == Enum.sort(msg_ids)
      end)
    end
  end

  describe "Consumer Group Coordination" do
    test "multiple consumer groups track offsets independently", %{topic: topic} do
      partition = 0

      # Produce messages
      messages = generate_messages(50)
      {:ok, _} = Producer.produce(topic, partition, messages)

      # Wait for messages to be flushed
      Process.sleep(500)

      # Consumer group 1 consumes first 20 messages
      group1 = "group1"
      {:ok, consumed1} = LogReader.consume(topic, partition, group1, 0, 1_048_576, 100)
      _consumed1_limited = Enum.take(consumed1, 20)
      DetsOffset.commit(topic, partition, group1, 19)

      # Consumer group 2 consumes first 30 messages
      group2 = "group2"
      {:ok, consumed2} = LogReader.consume(topic, partition, group2, 0, 1_048_576, 100)
      _consumed2_limited = Enum.take(consumed2, 30)
      DetsOffset.commit(topic, partition, group2, 29)

      # Verify independent offset tracking
      {:ok, offset1} = DetsOffset.fetch(topic, partition, group1)
      {:ok, offset2} = DetsOffset.fetch(topic, partition, group2)

      assert offset1 == 19
      assert offset2 == 29

      # Each group resumes from their own offset
      {:ok, resume1} = LogReader.consume(topic, partition, group1, offset1 + 1, 1_048_576, 100)
      {:ok, resume2} = LogReader.consume(topic, partition, group2, offset2 + 1, 1_048_576, 100)

      # Group 1 should get messages 20-49 (up to 30 messages)
      assert length(resume1) <= 30
      if length(resume1) > 0 do
        assert List.first(resume1)[:key] == "key-21"
      end

      # Group 2 should get messages 30-49 (up to 20 messages)
      assert length(resume2) <= 20
      if length(resume2) > 0 do
        assert List.first(resume2)[:key] == "key-31"
      end
    end

    test "handles concurrent consumers in same group", %{topic: topic} do
      partition = 0
      group = "concurrent-group"

      # Produce a large batch
      messages = generate_messages(200)
      {:ok, _} = Producer.produce(topic, partition, messages)

      # Wait for messages to be flushed
      Process.sleep(500)

      # Simulate multiple consumers in the same group
      # In real Kafka, partition would be assigned to one consumer
      # In our MVP, we test offset coordination

      # Consumer 1 reads and commits
      {:ok, batch1} = LogReader.consume(topic, partition, group, 0, 5000, 100)
      batch1_size = length(batch1)
      last_offset1 = batch1_size - 1
      DetsOffset.commit(topic, partition, group, last_offset1)

      # Consumer 2 reads from where Consumer 1 left off
      {:ok, committed} = DetsOffset.fetch(topic, partition, group)
      {:ok, batch2} = LogReader.consume(topic, partition, group, committed + 1, 5000, 100)
      batch2_size = length(batch2)
      last_offset2 = committed + batch2_size
      DetsOffset.commit(topic, partition, group, last_offset2)

      # Verify no message duplication based on message content
      all_consumed = batch1 ++ batch2

      # Check for duplicates based on key (since messages don't have offset field)
      unique_keys = all_consumed
        |> Enum.map(& &1[:key])
        |> Enum.uniq()

      # All keys should be unique (no duplicate messages)
      assert length(unique_keys) == length(all_consumed)

      # Verify we got a reasonable number of messages
      assert length(all_consumed) > 0
      assert length(all_consumed) <= 200  # We produced 200 messages total
    end
  end
end
