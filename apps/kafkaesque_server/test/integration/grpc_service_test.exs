defmodule Kafkaesque.Integration.GRPCServiceTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Kafkaesque.GRPC.Service
  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Test.{Factory, Helpers}
  alias Kafkaesque.Topic.LogReader
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor
  alias Kafkaesque.{
    CommitOffsetsRequest,
    CommitOffsetsResponse,
    CreateTopicRequest,
    GetOffsetsRequest,
    GetOffsetsResponse,
    ListTopicsResponse,
    ProduceRequest,
    ProduceResponse,
    Record,
    RecordHeader,
    Topic
  }

  setup_all do
    # Clean up all topics before running integration tests
    if Process.whereis(TopicSupervisor) do
      topics = TopicSupervisor.list_topics()
      Enum.each(topics, fn topic ->
        TopicSupervisor.delete_topic(topic.name)
      end)
    end
    :ok
  end

  setup do
    # Set up isolated test environment
    context = Helpers.setup_test()

    on_exit(fn ->
      Helpers.cleanup_test(context)
    end)

    {:ok, context: context}
  end

  describe "create_topic/2" do
    test "creates topic with valid parameters" do
      topic_name = Factory.unique_topic_name("grpc-topic")
      request = %CreateTopicRequest{name: topic_name, partitions: 3}
      result = Service.create_topic(request, nil)

      assert %Topic{} = result
      assert result.name == topic_name
      assert result.partitions == 3

      # Verify topic was actually created
      Helpers.assert_topic_exists(topic_name, 3)

      # Cleanup
      Helpers.delete_test_topic(topic_name)
    end

    test "defaults to 1 partition when 0 or negative specified" do
      topic_name = Factory.unique_topic_name("default")
      request = %CreateTopicRequest{name: topic_name, partitions: 0}
      result = Service.create_topic(request, nil)

      assert result.partitions == 1
      Helpers.assert_topic_exists(topic_name, 1)

      # Cleanup
      Helpers.delete_test_topic(topic_name)
    end

    test "raises error when topic creation fails" do
      topic_name = Factory.unique_topic_name("duplicate")
      # Create topic first
      request = %CreateTopicRequest{name: topic_name, partitions: 1}
      Service.create_topic(request, nil)

      # Try to create duplicate
      assert_raise GRPC.RPCError, fn ->
        Service.create_topic(request, nil)
      end

      # Cleanup
      Helpers.delete_test_topic(topic_name)
    end
  end

  describe "list_topics/2" do
    test "returns empty list when no topics exist" do
      # Clean up any existing topics first
      topics = Kafkaesque.Topic.Supervisor.list_topics()
      for topic <- topics do
        Helpers.delete_test_topic(topic.name)
      end
      Process.sleep(200)

      result = Service.list_topics(%Kafkaesque.ListTopicsRequest{}, nil)

      assert %ListTopicsResponse{} = result
      assert result.topics == []
    end

    test "returns all created topics" do
      # Create some topics with unique names
      {:ok, topic1, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("list1"), partitions: 1)
      {:ok, topic2, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("list2"), partitions: 2)
      {:ok, topic3, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("list3"), partitions: 3)

      result = Service.list_topics(%Kafkaesque.ListTopicsRequest{}, nil)

      assert %ListTopicsResponse{} = result
      assert length(result.topics) >= 3

      # Verify our topics are in the list
      topic_map = Map.new(result.topics, fn t -> {t.name, t.partitions} end)
      assert topic_map[topic1] == 1
      assert topic_map[topic2] == 2
      assert topic_map[topic3] == 3

      # Cleanup
      Helpers.delete_test_topic(topic1)
      Helpers.delete_test_topic(topic2)
      Helpers.delete_test_topic(topic3)
    end
  end

  describe "produce/2" do
    setup do
      {:ok, topic, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("produce"))
      on_exit(fn -> Helpers.delete_test_topic(topic) end)
      {:ok, topic: topic}
    end

    test "produces records successfully", %{topic: topic} do
      records = [
        %Record{
          key: "key1",
          value: "value1",
          headers: [%RecordHeader{key: "h1", value: "v1"}],
          timestamp_ms: System.system_time(:millisecond)
        },
        %Record{
          key: "key2",
          value: "value2",
          headers: [],
          timestamp_ms: System.system_time(:millisecond)
        }
      ]

      request = %ProduceRequest{
        topic: topic,
        partition: 0,
        records: records,
        acks: :ACKS_LEADER
      }

      result = Service.produce(request, nil)

      assert %ProduceResponse{} = result
      assert result.topic == topic
      assert result.partition == 0
      assert result.count == 2
      assert result.base_offset >= 0
    end

    test "uses partition 0 when negative partition specified", %{topic: topic} do
      request = %ProduceRequest{
        topic: topic,
        partition: -1,
        records: [%Record{key: "k", value: "v"}],
        acks: :ACKS_LEADER
      }

      result = Service.produce(request, nil)
      assert result.partition == 0
    end

    test "handles different ack levels", %{topic: topic} do
      record = %Record{key: "k", value: "v"}

      # Test ACKS_NONE
      request_none = %ProduceRequest{
        topic: topic,
        partition: 0,
        records: [record],
        acks: :ACKS_NONE
      }
      result_none = Service.produce(request_none, nil)
      assert result_none.count == 1

      # Test ACKS_LEADER
      request_leader = %ProduceRequest{
        topic: topic,
        partition: 0,
        records: [record],
        acks: :ACKS_LEADER
      }
      result_leader = Service.produce(request_leader, nil)
      assert result_leader.count == 1
    end

    test "raises error on backpressure", %{topic: topic} do
      # Create batches that will exceed queue capacity
      batch_size = 1000
      batch = Enum.map(1..batch_size, fn i ->
        %Record{key: "k#{i}", value: String.duplicate("x", 100)}
      end)

      request = %ProduceRequest{
        topic: topic,
        partition: 0,
        records: batch,
        acks: :ACKS_NONE  # Don't wait for acks
      }

      # Send multiple requests rapidly
      tasks = for _ <- 1..10 do
        Task.async(fn ->
          try do
            Service.produce(request, nil)
            :ok
          rescue
            e in GRPC.RPCError ->
              if e.message =~ "queue is full", do: :backpressure, else: :other_error
          end
        end)
      end

      results = Task.await_many(tasks, 10_000)

      # At least one should succeed or hit backpressure (not timeout)
      assert Enum.any?(results, fn r -> r in [:ok, :backpressure] end)
    end
  end

  describe "get_offsets/2" do
    setup do
      {:ok, topic, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("offset"))
      on_exit(fn -> Helpers.delete_test_topic(topic) end)
      {:ok, topic: topic}
    end

    test "returns correct offsets for empty topic", %{topic: topic} do
      request = %GetOffsetsRequest{topic: topic, partition: 0}
      result = Service.get_offsets(request, nil)

      assert %GetOffsetsResponse{} = result
      assert result.earliest == 0
      assert result.latest == 0
    end

    test "returns correct offsets after producing messages", %{topic: topic} do
      # Produce some messages
      messages = Factory.generate_messages(10)
      {:ok, produce_result} = Helpers.produce_and_wait(topic, 0, messages)

      # Wait for messages to be fully written
      Process.sleep(100)

      request = %GetOffsetsRequest{topic: topic, partition: 0}
      result = Service.get_offsets(request, nil)

      assert result.earliest == 0
      assert result.latest == produce_result.base_offset + 10  # Next offset to write
    end

    test "handles non-existent topic gracefully" do
      request = %GetOffsetsRequest{topic: "non-existent", partition: 0}
      result = Service.get_offsets(request, nil)

      # Should return defaults
      assert result.earliest == 0
      assert result.latest == 0
    end
  end

  describe "commit_offsets/2" do
    setup do
      {:ok, topic, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("commit"), partitions: 2)
      on_exit(fn -> Helpers.delete_test_topic(topic) end)
      {:ok, topic: topic}
    end

    test "commits offsets successfully", %{topic: topic} do
      offsets = [
        %CommitOffsetsRequest.Offset{topic: topic, partition: 0, offset: 10},
        %CommitOffsetsRequest.Offset{topic: topic, partition: 1, offset: 20}
      ]

      request = %CommitOffsetsRequest{
        group: "test-group",
        offsets: offsets
      }

      result = Service.commit_offsets(request, nil)

      assert %CommitOffsetsResponse{} = result

      # Verify offsets were committed
      {:ok, offset0} = DetsOffset.fetch(topic, 0, "test-group")
      {:ok, offset1} = DetsOffset.fetch(topic, 1, "test-group")

      assert offset0 == 10
      assert offset1 == 20
    end

    test "handles empty offset list" do
      request = %CommitOffsetsRequest{
        group: "test-group",
        offsets: []
      }

      result = Service.commit_offsets(request, nil)
      assert %CommitOffsetsResponse{} = result
    end
  end

  describe "record conversion" do
    setup do
      {:ok, topic, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("conversion"))
      on_exit(fn -> Helpers.delete_test_topic(topic) end)
      {:ok, topic: topic}
    end

    test "converts protobuf record to internal format correctly", %{topic: topic} do
      pb_record = %Record{
        key: "test-key",
        value: "test-value",
        headers: [
          %RecordHeader{key: "h1", value: "v1"},
          %RecordHeader{key: "h2", value: "v2"}
        ],
        timestamp_ms: 1_234_567_890
      }

      # Use private function through produce
      request = %ProduceRequest{
        topic: topic,
        partition: 0,
        records: [pb_record],
        acks: :ACKS_LEADER
      }

      result = Service.produce(request, nil)

      assert result.count == 1

      # Consume to verify conversion
      {:ok, consumed} = Helpers.consume_messages(topic, 0, 0)
      msg = List.first(consumed)

      assert msg[:key] == "test-key"
      assert msg[:value] == "test-value"
      assert {"h1", "v1"} in msg[:headers]
      assert {"h2", "v2"} in msg[:headers]
      assert msg[:timestamp_ms] == 1_234_567_890
    end

    test "handles empty headers and missing timestamp", %{topic: topic} do
      pb_record = %Record{
        key: "key",
        value: "value",
        headers: [],
        timestamp_ms: 0
      }

      request = %ProduceRequest{
        topic: topic,
        partition: 0,
        records: [pb_record],
        acks: :ACKS_LEADER
      }

      result = Service.produce(request, nil)

      assert result.count == 1

      # Consume to verify
      {:ok, consumed} = Helpers.consume_messages(topic, 0, 0)
      msg = List.first(consumed)

      assert msg[:key] == "key"
      assert msg[:value] == "value"
      assert msg[:headers] == []
      # Timestamp should be auto-generated when 0
      assert msg[:timestamp_ms] > 0
    end
  end

  describe "consume streaming" do
    setup do
      # Use unique topic name for test isolation
      {:ok, topic_name, _} = Helpers.create_test_topic(name: Factory.unique_topic_name("stream"))

      # Produce test messages
      messages = Factory.generate_messages(20)
      {:ok, result} = Helpers.produce_and_wait(topic_name, 0, messages)

      # Wait a bit more for messages to be fully written
      Process.sleep(200)

      on_exit(fn -> Helpers.delete_test_topic(topic_name) end)
      {:ok, topic: topic_name, last_offset: result.base_offset + 19}
    end

    test "handles offset specifications correctly", %{topic: topic, last_offset: last_offset} do
      # Test non-streaming consume behavior through produce/consume cycle
      # Since we're testing the service implementation, we'll verify offset handling
      # by checking the actual consumption behavior

      # Verify we can consume from latest offset
      {:ok, initial_offsets} = SingleFile.get_offsets(topic, 0)
      assert initial_offsets.latest == last_offset + 1  # Next offset to write

      # Produce additional messages
      new_messages = Factory.generate_messages(5, key_prefix: "latest")
      {:ok, _produce_result} = Helpers.produce_and_wait(topic, 0, new_messages)

      # Wait a bit more to ensure messages are fully available
      Process.sleep(300)

      # Consume all messages to find the new ones
      {:ok, all_consumed} = Helpers.consume_messages(topic, 0, 0, 100)

      # Filter to get only the new messages (those with "latest" prefix)
      new_consumed = Enum.filter(all_consumed, fn msg ->
        String.starts_with?(msg[:key] || "", "latest")
      end)

      assert length(new_consumed) == 5
      assert List.first(new_consumed)[:key] == "latest-1"
    end

    test "respects consumer group committed offset", %{topic: topic} do
      # Use unique consumer group to avoid conflicts
      group = Factory.unique_group_name("stream-group")

      # Commit an offset for a group
      DetsOffset.commit(topic, 0, group, 9)

      # Test that fetch with consumer group uses committed offset
      {:ok, committed} = DetsOffset.fetch(topic, 0, group)
      assert committed == 9

      # Consume starting from committed offset
      {:ok, consumed} = LogReader.consume(topic, 0, group, 10, 1_048_576, 100)

      # Should get messages starting from offset 10
      assert length(consumed) == 10  # Messages 10-19
      assert List.first(consumed)[:key] == "key-11"  # 0-indexed, so offset 10 = key-11
    end
  end

  describe "error handling" do
    test "produce handles invalid topic gracefully" do
      request = %ProduceRequest{
        topic: "non-existent-topic",
        partition: 0,
        records: [%Record{key: "k", value: "v"}],
        acks: :ACKS_LEADER
      }

      assert_raise GRPC.RPCError, fn ->
        Service.produce(request, nil)
      end
    end

    test "handles malformed requests" do
      # Test with empty topic string instead of nil
      request = %ProduceRequest{
        topic: "",
        partition: 0,
        records: [],
        acks: :ACKS_LEADER
      }

      assert_raise GRPC.RPCError, fn ->
        Service.produce(request, nil)
      end
    end
  end
end
