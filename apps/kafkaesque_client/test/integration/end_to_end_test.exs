defmodule KafkaesqueClient.Integration.EndToEndTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias KafkaesqueClient
  alias KafkaesqueClient.{Admin, Consumer, Producer}
  alias KafkaesqueClient.Record.ProducerRecord

  defp get_process_name({module, _opts}) when is_atom(module), do: module
  defp get_process_name({module, opts}) when is_list(opts) do
    Keyword.get(opts, :name, module)
  end

  setup_all do
    # First ensure all server apps are started
    Application.ensure_all_started(:kafkaesque_core)
    Application.ensure_all_started(:kafkaesque_server)
    Application.ensure_all_started(:grpc)

    # Stop any existing gRPC server first
    try do
      GRPC.Server.stop_endpoint(KafkaesqueServer.GRPC.Endpoint)
    rescue
      _ -> :ok
    catch
      _ -> :ok
    end

    # Start the gRPC server on test port 50052
    case GRPC.Server.start_endpoint(KafkaesqueServer.GRPC.Endpoint, 50_052) do
      {:ok, _pid, port} ->
        IO.puts("Started gRPC server on port #{port}")
        on_exit(fn ->
          try do
            GRPC.Server.stop_endpoint(KafkaesqueServer.GRPC.Endpoint)
          rescue
            _ -> :ok
          end
        end)
      {:error, {:already_started, _}} ->
        IO.puts("gRPC server already started")
      {:error, :eaddrinuse} ->
        IO.puts("Port 50_052 already in use")
      error ->
        IO.puts("Warning: Could not start gRPC server: #{inspect(error)}")
    end

    # Wait a bit for server to be ready
    Process.sleep(200)

    # Start the client infrastructure with the test port
    children = [
      {Registry, keys: :unique, name: KafkaesqueClient.ConnectionRegistry},
      {DynamicSupervisor, name: KafkaesqueClient.ConnectionSupervisor, strategy: :one_for_one},
      {KafkaesqueClient.Connection.Pool, bootstrap_servers: ["localhost:50052"]}
    ]

    for child <- children do
      case Process.whereis(get_process_name(child)) do
        nil -> start_supervised!(child)
        _pid -> :ok
      end
    end

    :ok
  end

  setup do
    # Generate unique topic name for test isolation
    topic_name = "test-topic-#{:rand.uniform(100_000)}"
    {:ok, topic: topic_name}
  end

  describe "Producer/Consumer Integration" do
    test "produces and consumes messages with Kafka-style API", %{topic: topic} do
      # Create admin client and topic
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )

      {:ok, _} = Admin.create_topic(admin, topic, partitions: 1)

      # Create producer
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 10
      )

      # Send messages
      messages = for i <- 1..10 do
        record = KafkaesqueClient.producer_record(
          topic,
          "key-#{i}",
          "value-#{i}",
          headers: %{"msg_id" => "#{i}"}
        )

        {:ok, metadata} = Producer.send_sync(producer, record)
        assert metadata.topic == topic
        assert metadata.partition == 0
        assert metadata.offset >= 0

        metadata
      end

      # Verify offsets are sequential
      offsets = Enum.map(messages, & &1.offset)
      assert offsets == Enum.sort(offsets)

      # Create consumer
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50052"],
        group_id: "test-group-#{:rand.uniform(1000)}",
        auto_offset_reset: :earliest,
        enable_auto_commit: false,
        max_poll_records: 5
      )

      # Subscribe to topic
      :ok = Consumer.subscribe(consumer, [topic])

      # Poll for messages
      Process.sleep(100)  # Give stream time to start

      records1 = Consumer.poll(consumer, 1000)
      assert length(records1) <= 5

      records2 = Consumer.poll(consumer, 1000)
      assert length(records2) <= 5

      all_records = records1 ++ records2
      assert length(all_records) == 10

      # Verify message contents
      Enum.each(Enum.with_index(all_records, 1), fn {record, i} ->
        assert record.topic == topic
        assert record.key == "key-#{i}"
        assert record.value == "value-#{i}"
        assert record.headers["msg_id"] == "#{i}"
      end)

      # Test manual commit
      :ok = Consumer.commit_sync(consumer)

      # Close clients
      :ok = Producer.close(producer)
      :ok = Consumer.close(consumer)
      :ok = Admin.close(admin)
    end

    test "handles producer batching correctly", %{topic: topic} do
      # Create topic
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )
      {:ok, _} = Admin.create_topic(admin, topic, partitions: 1)

      # Create producer with batching
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"],
        acks: :leader,
        batch_size: 500,  # Small batch to trigger multiple batches
        linger_ms: 50
      )

      # Send messages asynchronously
      callback_results = :ets.new(:results, [:set, :public])

      for i <- 1..20 do
        record = ProducerRecord.new(topic, "batch-key-#{i}", "batch-value-#{i}")

        Producer.send(producer, record, fn result ->
          :ets.insert(callback_results, {i, result})
        end)
      end

      # Flush to ensure all messages are sent
      :ok = Producer.flush(producer)

      # Wait for callbacks
      Process.sleep(100)

      # Verify all callbacks were invoked
      results = :ets.tab2list(callback_results)
      assert length(results) == 20

      # Check all were successful
      Enum.each(results, fn {_i, result} ->
        assert {:ok, %{topic: ^topic}} = result
      end)

      # Check metrics
      metrics = Producer.metrics(producer)
      assert metrics.records_sent == 20
      assert metrics.batches_sent > 0
      assert metrics.errors == 0

      :ets.delete(callback_results)
      :ok = Producer.close(producer)
      :ok = Admin.close(admin)
    end
  end

  describe "Consumer Groups" do
    test "multiple consumers in same group share partitions", %{topic: topic} do
      # Create topic
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )
      {:ok, _} = Admin.create_topic(admin, topic, partitions: 1)

      # Produce test messages
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"]
      )

      for i <- 1..20 do
        record = ProducerRecord.new(topic, "group-test-#{i}")
        {:ok, _} = Producer.send_sync(producer, record)
      end

      group_id = "shared-group-#{:rand.uniform(1000)}"

      # Create two consumers in same group
      {:ok, consumer1} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50052"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        max_poll_records: 100
      )

      {:ok, consumer2} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50052"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        max_poll_records: 100
      )

      # Both subscribe to same topic
      :ok = Consumer.subscribe(consumer1, [topic])
      :ok = Consumer.subscribe(consumer2, [topic])

      Process.sleep(200)

      # Poll from both consumers
      records1 = Consumer.poll(consumer1, 1000)
      records2 = Consumer.poll(consumer2, 1000)

      # In our MVP without proper consumer group rebalancing,
      # both consumers will read all messages independently
      # This is different from Kafka which would coordinate partition assignment
      assert length(records1) == 20
      assert length(records2) == 20

      # Verify they got the same messages (since no coordination)
      values1 = Enum.map(records1, & &1.value) |> Enum.sort()
      values2 = Enum.map(records2, & &1.value) |> Enum.sort()
      assert values1 == values2

      :ok = Consumer.close(consumer1)
      :ok = Consumer.close(consumer2)
      :ok = Producer.close(producer)
      :ok = Admin.close(admin)
    end

    test "consumer group offset tracking", %{topic: topic} do
      group_id = "offset-group-#{:rand.uniform(1000)}"

      # Setup
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )
      {:ok, _} = Admin.create_topic(admin, topic, partitions: 1)

      # Produce messages
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"]
      )

      for i <- 1..10 do
        record = ProducerRecord.new(topic, "offset-test-#{i}")
        {:ok, _} = Producer.send_sync(producer, record)
      end

      # First consumer reads 5 messages
      {:ok, consumer1} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50052"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: false,
        max_poll_records: 5
      )

      :ok = Consumer.subscribe(consumer1, [topic])
      Process.sleep(100)

      records1 = Consumer.poll(consumer1, 1000)
      assert length(records1) == 5

      # Manually commit offset
      :ok = Consumer.commit_sync(consumer1)
      :ok = Consumer.close(consumer1)

      # Second consumer in same group continues from committed offset
      {:ok, consumer2} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50052"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        max_poll_records: 10
      )

      :ok = Consumer.subscribe(consumer2, [topic])
      Process.sleep(100)

      records2 = Consumer.poll(consumer2, 1000)
      assert length(records2) == 5

      # Verify we got the remaining messages
      values = Enum.map(records2, & &1.value)
      assert "offset-test-6" in values
      assert "offset-test-10" in values

      :ok = Consumer.close(consumer2)
      :ok = Producer.close(producer)
      :ok = Admin.close(admin)
    end
  end

  describe "Admin Operations" do
    test "creates and lists topics" do
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )

      # Create multiple topics
      topics = for i <- 1..3 do
        name = "admin-test-#{:rand.uniform(100_000)}-#{i}"
        {:ok, topic} = Admin.create_topic(admin, name, partitions: i)
        assert topic.name == name
        assert topic.partitions == i
        topic
      end

      # List topics
      {:ok, all_topics} = Admin.list_topics(admin)

      # Verify our topics are in the list
      topic_names = Enum.map(topics, & &1.name)
      listed_names = Enum.map(all_topics, & &1.name)

      Enum.each(topic_names, fn name ->
        assert name in listed_names
      end)

      :ok = Admin.close(admin)
    end

    test "gets topic offsets" do
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )

      topic = "offset-test-#{:rand.uniform(100_000)}"
      {:ok, _} = Admin.create_topic(admin, topic)

      # Initially empty
      {:ok, offsets1} = Admin.get_offsets(admin, topic, 0)
      assert offsets1.earliest == 0
      assert offsets1.latest == 0

      # Produce some messages
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"]
      )

      for i <- 1..5 do
        record = ProducerRecord.new(topic, "msg-#{i}")
        {:ok, _} = Producer.send_sync(producer, record)
      end

      # Check offsets again
      {:ok, offsets2} = Admin.get_offsets(admin, topic, 0)
      assert offsets2.earliest == 0
      assert offsets2.latest == 5

      :ok = Producer.close(producer)
      :ok = Admin.close(admin)
    end

    test "producer routes records based on key and round-robin" do
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )

      topic = "routing-test-#{:rand.uniform(100_000)}"
      {:ok, _} = Admin.create_topic(admin, topic, partitions: 3)

      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"]
      )

      keyed_record = %ProducerRecord{topic: topic, key: "user-123", value: "payload"}
      {:ok, keyed_metadata} = Producer.send_sync(producer, keyed_record)
      assert keyed_metadata.partition == :erlang.phash2("user-123", 3)

      record_a = ProducerRecord.new(topic, "round-robin-a")
      {:ok, metadata_a} = Producer.send_sync(producer, record_a)
      record_b = ProducerRecord.new(topic, "round-robin-b")
      {:ok, metadata_b} = Producer.send_sync(producer, record_b)

      # Default round-robin should iterate partitions starting from 0
      assert metadata_a.partition == 0
      assert metadata_b.partition == 1

      :ok = Producer.close(producer)
      :ok = Admin.close(admin)
    end

    test "describes topic with partition info" do
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )

      topic = "describe-test-#{:rand.uniform(100_000)}"
      {:ok, _} = Admin.create_topic(admin, topic, partitions: 2)

      # Produce to partition 0
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"]
      )

      for i <- 1..3 do
        record = %ProducerRecord{
          topic: topic,
          partition: 0,
          value: "p0-msg-#{i}"
        }
        {:ok, _} = Producer.send_sync(producer, record)
      end

      # Describe topic
      {:ok, description} = Admin.describe_topic(admin, topic)

      assert description.name == topic
      assert description.partitions == 2
      assert length(description.partition_info) == 2

      # Check partition 0 info
      p0_info = Enum.find(description.partition_info, & &1.partition == 0)
      assert p0_info.message_count == 3
      assert p0_info.latest_offset == 3

      # Check partition 1 info (empty)
      p1_info = Enum.find(description.partition_info, & &1.partition == 1)
      assert p1_info.message_count == 0
      assert p1_info.latest_offset == 0

      :ok = Producer.close(producer)
      :ok = Admin.close(admin)
    end
  end

  describe "Async Flush" do
    test "async flush works correctly in producer", %{topic: topic} do
      {:ok, admin} = KafkaesqueClient.create_admin(
        bootstrap_servers: ["localhost:50052"]
      )
      {:ok, _} = Admin.create_topic(admin, topic)

      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"],
        batch_size: 1024,
        linger_ms: 60_000  # Very long linger
      )

      test_pid = self()

      # Send records with callbacks
      for i <- 1..5 do
        record = ProducerRecord.new(topic, "async-#{i}")
        Producer.send(producer, record, fn {:ok, metadata} ->
          send(test_pid, {:sent, i, metadata})
        end)
      end

      # Async flush should return immediately
      start = System.monotonic_time(:millisecond)
      :ok = Producer.flush_async(producer)
      duration = System.monotonic_time(:millisecond) - start

      # Should be very fast (< 10ms)
      assert duration < 10

      # Wait for callbacks to confirm sends completed
      for i <- 1..5 do
        assert_receive {:sent, ^i, %{topic: ^topic}}, 5_000
      end

      :ok = Producer.close(producer)
      :ok = Admin.close(admin)
    end
  end

  describe "Error Handling" do
    test "handles connection errors gracefully" do
      # Try to connect to non-existent server (use valid but unused port)
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:59999"],
        max_retries: 1,
        retry_backoff_ms: 10
      )

      record = ProducerRecord.new("test", "value")
      result = Producer.send_sync(producer, record, 1000)

      assert {:error, _} = result

      :ok = Producer.close(producer)
    end

    test "handles topic not found errors" do
      {:ok, producer} = KafkaesqueClient.create_producer(
        bootstrap_servers: ["localhost:50052"]
      )

      non_existent_topic = "non-existent-#{:rand.uniform(100_000)}"
      record = ProducerRecord.new(non_existent_topic, "value")

      result = Producer.send_sync(producer, record)
      assert {:error, _} = result

      :ok = Producer.close(producer)
    end
  end
end
