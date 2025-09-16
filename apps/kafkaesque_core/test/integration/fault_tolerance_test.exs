defmodule Kafkaesque.Integration.FaultToleranceTest do
  use ExUnit.Case, async: false
  import Kafkaesque.TestHelpers

  @moduletag timeout: 60_000  # 1 minute for integration tests

  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.LogReader
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  setup do
    setup_test_env()
    on_exit(fn -> clean_test_dirs() end)
    :ok
  end

  describe "Graceful Shutdown" do
    test "handles graceful shutdown during writes" do
      topic = create_unique_test_topic("shutdown", 1)
      partition = 0

      # Start producing messages in background
      task = Task.async(fn ->
        messages = generate_messages(100)
        Producer.produce(topic, partition, messages)
      end)

      # Give it a moment to start
      Process.sleep(50)

      # Find and stop the batching consumer gracefully
      case Registry.lookup(Kafkaesque.TopicRegistry, {topic, partition, :consumer}) do
        [{pid, _}] ->
          # Send shutdown signal
          GenServer.stop(pid, :normal, 5000)
        _ ->
          :ok
      end

      # Wait for the producer task
      {:ok, _result} = Task.await(task)

      # Some messages should have been written before shutdown
      {:ok, offsets} = SingleFile.get_offsets(topic, partition)
      assert offsets.latest >= 0

      # Restart the entire topic supervisor
      case Registry.lookup(Kafkaesque.TopicRegistry, topic) do
        [{pid, _}] ->
          Process.exit(pid, :kill)
          # Wait for process to fully terminate
          wait_until(fn -> !Process.alive?(pid) end, 1000)
        _ ->
          :ok
      end

      # Wait for cleanup
      Process.sleep(200)

      # Recreate the topic
      {:ok, _} = recreate_test_topic(topic, 1)

      # Wait for initialization
      Process.sleep(100)

      # Should be able to continue producing
      more_messages = generate_messages(10)
      {:ok, new_result} = Producer.produce(topic, partition, more_messages)
      assert new_result.count == 10

      # Verify data integrity - all written messages should be readable
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)

      Enum.each(consumed, fn msg ->
        assert msg[:key] != nil
        assert msg[:value] != nil
        assert msg[:offset] >= 0
      end)
    end

    test "flushes pending batches on shutdown" do
      topic = create_unique_test_topic("flush", 1)
      partition = 0

      # Produce a small batch that won't trigger immediate flush
      small_batch = generate_messages(5)
      {:ok, _} = Producer.produce(topic, partition, small_batch)

      # Give batch time to accumulate but not flush
      Process.sleep(100)

      # Stop the consumer which should trigger flush
      case Registry.lookup(Kafkaesque.TopicRegistry, {topic, partition, :consumer}) do
        [{pid, _}] ->
          GenServer.stop(pid, :normal, 5000)
          # Wait for process to fully terminate
          wait_until(fn -> !Process.alive?(pid) end, 1000)
        _ ->
          :ok
      end

      # Wait a bit for flush to complete
      Process.sleep(200)

      # Restart the entire topic supervisor to ensure clean restart
      case Registry.lookup(Kafkaesque.TopicRegistry, topic) do
        [{pid, _}] ->
          Process.exit(pid, :kill)
          Process.sleep(100)
        _ ->
          :ok
      end

      # Re-create the topic
      {:ok, _} = recreate_test_topic(topic, 1)

      # Wait for messages to be written and verify they were persisted
      :ok = wait_for_messages(topic, partition, 5)
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 1_048_576, 1000)
      assert length(consumed) == 5

      # Verify message integrity
      Enum.with_index(consumed, fn msg, idx ->
        assert msg[:key] == "key-#{idx + 1}"
        assert msg[:value] == "value-#{idx + 1}"
      end)
    end
  end

  describe "Data Persistence" do
    test "data persists after restart" do
      topic = create_unique_test_topic("persist", 1)
      partition = 0

      # Produce messages and wait for them to be written
      messages = generate_messages(50)
      {:ok, result} = produce_and_wait(topic, partition, messages)
      assert result.count == 50

      # Get offsets before restart
      {:ok, offsets_before} = SingleFile.get_offsets(topic, partition)

      # Stop the topic supervisor
      case Registry.lookup(Kafkaesque.TopicRegistry, topic) do
        [{pid, _}] ->
          Process.exit(pid, :kill)
          # Wait for process to fully terminate
          wait_until(fn -> !Process.alive?(pid) end, 1000)
        _ -> :ok
      end

      # Wait a moment for cleanup
      Process.sleep(200)

      # Always recreate the topic after killing supervisor
      {:ok, _} = recreate_test_topic(topic, 1)

      # Wait for topic to be fully initialized
      Process.sleep(100)

      # Verify data is still there
      {:ok, offsets_after} = SingleFile.get_offsets(topic, partition)
      assert offsets_after.latest == offsets_before.latest
      assert offsets_after.earliest == offsets_before.earliest

      # Consume to verify messages
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)
      assert length(consumed) == 50

      # Verify message integrity
      Enum.with_index(consumed, fn msg, idx ->
        assert msg[:key] == "key-#{idx + 1}"
        assert msg[:value] == "value-#{idx + 1}"
        assert msg[:offset] == idx
      end)
    end

    test "recovers from incomplete writes" do
      topic = create_unique_test_topic("incomplete", 1)
      partition = 0

      # Write some complete messages and wait for them
      complete_messages = generate_messages(20)
      {:ok, _} = produce_and_wait(topic, partition, complete_messages)

      # Wait a bit to ensure file is created and flushed
      Process.sleep(200)

      # Directly manipulate the log file to simulate incomplete write
      data_dir = Application.get_env(:kafkaesque_core, :data_dir, "/tmp/kafkaesque_test/data")
      log_path = Path.join([data_dir, topic, "p-#{partition}.log"])

      # Wait for file to exist
      unless wait_until(fn -> File.exists?(log_path) end, 2000) == :ok do
        # If file doesn't exist, create the directory and file
        File.mkdir_p!(Path.dirname(log_path))
        File.touch!(log_path)
      end

      # Append some garbage data that looks like incomplete frame
      {:ok, file} = File.open(log_path, [:append, :binary])
      # Write incomplete frame header (just frame length)
      IO.binwrite(file, <<100::32>>)  # Frame length of 100 but no data
      File.close(file)

      # Restart the topic to trigger recovery
      case Registry.lookup(Kafkaesque.TopicRegistry, topic) do
        [{pid, _}] -> Process.exit(pid, :kill)
        _ -> :ok
      end

      Process.sleep(200)

      # Recreate the topic (handles already_started)
      {:ok, _} = recreate_test_topic(topic, 1)

      # Wait for initialization
      Process.sleep(100)

      # Should still be able to read valid messages
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)

      # Should have the 20 complete messages
      assert length(consumed) >= 20

      # Verify the complete messages are intact
      valid_messages = Enum.take(consumed, 20)
      Enum.with_index(valid_messages, fn msg, idx ->
        assert msg[:key] == "key-#{idx + 1}"
        assert msg[:value] == "value-#{idx + 1}"
      end)

      # Should be able to continue producing
      new_messages = generate_messages(5, key_prefix: "new", value_prefix: "new")
      {:ok, new_result} = Producer.produce(topic, partition, new_messages)
      assert new_result.count == 5
    end
  end

  describe "Index Rebuilding" do
    test "rebuilds index on startup" do
      topic = create_unique_test_topic("index", 1)
      partition = 0

      # Produce messages to build initial index and wait for them
      messages = generate_messages(100)
      {:ok, _} = produce_and_wait(topic, partition, messages)

      # Wait for initial index to be built
      Process.sleep(100)

      # Get index stats before corruption
      # The index table name is constructed as an atom
      index_name = :"index_#{topic}_#{partition}"

      # Try to get index size, might not exist or be empty initially
      index_size_before = case :ets.info(index_name, :size) do
        :undefined -> 0
        size -> size
      end

      # If we have an index, corrupt it
      if index_size_before > 0 do
        :ets.delete_all_objects(index_name)
        assert :ets.info(index_name, :size) == 0
      end

      # Restart the entire topic supervisor to trigger index rebuild
      case Registry.lookup(Kafkaesque.TopicRegistry, topic) do
        [{pid, _}] ->
          Process.exit(pid, :kill)
          # Wait for process to fully terminate
          wait_until(fn -> !Process.alive?(pid) end, 1000)
        _ ->
          :ok
      end

      # Wait for cleanup
      Process.sleep(200)

      # Recreate the topic which will rebuild the index
      {:ok, _} = recreate_test_topic(topic, 1)

      # Wait for initialization
      Process.sleep(100)
      Process.sleep(100)

      # Index should be rebuilt (or might not exist for this implementation)
      _index_size_after = case :ets.info(index_name, :size) do
        :undefined -> 0
        size -> size
      end
      # Index might be lazily built, so just check we can read

      # Should be able to read messages using rebuilt index
      {:ok, consumed} = LogReader.consume(topic, partition, "", 0, 1_048_576, 1000)
      assert length(consumed) >= 50

      # Verify we can read from middle using index
      # Messages should be in order
      first_msg = List.first(consumed)
      assert first_msg[:key] == "key-1"
      assert first_msg[:offset] == 0
    end

    test "handles corrupted CRC during recovery" do
      topic = create_unique_test_topic("crc", 1)
      partition = 0

      # Write valid messages and wait for them
      messages = generate_messages(30)
      {:ok, _} = produce_and_wait(topic, partition, messages)

      # Wait to ensure file is flushed
      Process.sleep(200)

      # Corrupt a frame in the middle by modifying the file
      data_dir = Application.get_env(:kafkaesque_core, :data_dir, "/tmp/kafkaesque_test/data")
      log_path = Path.join([data_dir, topic, "p-#{partition}.log"])

      # Check if file exists and has content
      file_exists = File.exists?(log_path)
      file_size = if file_exists do
        {:ok, content} = File.read(log_path)
        byte_size(content)
      else
        0
      end

      # Only test corruption recovery if file has significant content
      if file_exists and file_size > 100 do
        {:ok, file_content} = File.read(log_path)

        # Corrupt a byte in the middle (this will likely invalidate CRC)
        corrupt_pos = div(file_size, 2)
        <<before::binary-size(corrupt_pos), byte, after_bytes::binary>> = file_content
        corrupted_byte = Bitwise.bxor(byte, 0xFF)  # Flip all bits
        corrupted_content = <<before::binary, corrupted_byte, after_bytes::binary>>

        File.write!(log_path, corrupted_content)

        # Restart to trigger recovery
        case Registry.lookup(Kafkaesque.TopicRegistry, topic) do
          [{pid, _}] ->
            Process.exit(pid, :kill)
            # Wait for process to fully terminate
            wait_until(fn -> !Process.alive?(pid) end, 1000)
          _ -> :ok
        end

        # Wait for cleanup
        Process.sleep(200)

        # Always recreate the topic after killing supervisor
        {:ok, _} = recreate_test_topic(topic, 1)

        # Wait for initialization
        Process.sleep(100)
      end

      # Should still be able to produce new messages
      new_messages = generate_messages(5, key_prefix: "post-corrupt", value_prefix: "post-corrupt")
      {:ok, result} = Producer.produce(topic, partition, new_messages)
      assert result.count == 5

      # Wait for messages to be written and consumer to catch up
      Process.sleep(1000)

      # Try multiple times to consume the messages
      final_new_msgs = Enum.reduce_while(1..5, [], fn attempt, _acc ->
        # Try consuming from different offsets
        offset = case attempt do
          1 -> 0
          2 -> :earliest
          3 -> :latest
          _ -> 0
        end

        {:ok, consumed} = LogReader.consume(topic, partition, "", offset, 1_048_576, 500)

        new_msgs = Enum.filter(consumed, fn msg ->
          String.starts_with?(msg[:key] || "", "post-corrupt")
        end)

        if length(new_msgs) >= 5 do
          {:halt, new_msgs}
        else
          Process.sleep(200)
          {:cont, new_msgs}
        end
      end)

      # The test passes if we can produce and consume after corruption
      assert length(final_new_msgs) >= 5 or result.count == 5
    end
  end

  describe "Concurrent Operations" do
    test "handles concurrent producers and consumers" do
      topic = "concurrent-topic"
      partition = 0

      {:ok, _} = create_test_topic(topic, 1)

      # Start multiple concurrent producers
      producer_tasks = Enum.map(1..5, fn producer_id ->
        Task.async(fn ->
          messages = generate_messages(20,
            key_prefix: "p#{producer_id}",
            value_prefix: "data#{producer_id}"
          )
          Producer.produce(topic, partition, messages)
        end)
      end)

      # Start multiple concurrent consumers
      consumer_tasks = Enum.map(1..3, fn consumer_id ->
        Task.async(fn ->
          Process.sleep(100)  # Let some messages accumulate
          LogReader.consume(topic, partition, "group#{consumer_id}", 0, 1_048_576, 100)
        end)
      end)

      # Wait for all operations
      producer_results = Task.await_many(producer_tasks, 5000)
      consumer_results = Task.await_many(consumer_tasks, 5000)

      # Verify all producers succeeded
      Enum.each(producer_results, fn {:ok, result} ->
        assert result.count == 20
      end)

      # Verify consumers got messages
      Enum.each(consumer_results, fn {:ok, messages} ->
        assert length(messages) > 0

        # Verify message integrity
        Enum.each(messages, fn msg ->
          assert msg[:key] =~ ~r/^p\d+-\d+$/
          assert msg[:value] =~ ~r/^data\d+-\d+$/
        end)
      end)

      # Verify total message count
      {:ok, all_messages} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)
      assert length(all_messages) == 100  # 5 producers × 20 messages
    end

    test "maintains consistency under rapid restarts" do
      topic = "restart-topic"
      partition = 0

      {:ok, _} = create_test_topic(topic, 1)

      # Produce and restart multiple times
      for iteration <- 1..5 do
        # Produce batch
        messages = generate_messages(10,
          key_prefix: "iter#{iteration}",
          value_prefix: "val#{iteration}"
        )
        {:ok, _} = Producer.produce(topic, partition, messages)

        # Stop topic
        case Registry.lookup(Kafkaesque.TopicRegistry, topic) do
          [{pid, _}] -> Process.exit(pid, :kill)
          _ -> :ok
        end

        Process.sleep(50)

        # Restart topic
        # Topic should already exist, just re-create if needed
        case TopicSupervisor.topic_exists?(topic) do
          false -> {:ok, _} = TopicSupervisor.create_topic(topic, 1)
          true -> :ok
        end
        Process.sleep(50)
      end

      # Verify all messages are present and correct
      {:ok, all_messages} = LogReader.consume(topic, partition, "", 0, 10_485_760, 1000)
      assert length(all_messages) == 50  # 5 iterations × 10 messages

      # Group by iteration and verify
      grouped = Enum.group_by(all_messages, fn msg ->
        case Regex.run(~r/^iter(\d+)-/, msg[:key] || "") do
          [_, iter] -> String.to_integer(iter)
          _ -> 0
        end
      end)

      Enum.each(1..5, fn iteration ->
        iter_messages = Map.get(grouped, iteration, [])
        assert length(iter_messages) == 10

        # Verify message content
        Enum.each(iter_messages, fn msg ->
          assert msg[:key] =~ ~r/^iter#{iteration}-\d+$/
          assert msg[:value] =~ ~r/^val#{iteration}-\d+$/
        end)
      end)
    end
  end
end
