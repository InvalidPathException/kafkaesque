defmodule Kafkaesque.Pipeline.MessageBufferTest do
  use ExUnit.Case, async: false

  alias Kafkaesque.Pipeline.MessageBuffer

  setup_all do
    case Registry.start_link(keys: :unique, name: Kafkaesque.TopicRegistry) do
      {:ok, pid} ->
        on_exit(fn -> Process.exit(pid, :normal) end)
        {:ok, registry: pid}

      {:error, {:already_started, pid}} ->
        {:ok, registry: pid}
    end
  end

  @test_topic "buffer_test"
  @test_partition 0

  defp create_test_records(count, prefix \\ "test") do
    Enum.map(1..count, fn i ->
      %{
        key: "#{prefix}_key_#{i}",
        value: "#{prefix}_value_#{i}",
        headers: [{"header_#{i}", "value_#{i}"}],
        timestamp_ms: System.system_time(:millisecond)
      }
    end)
  end

  describe "start_link/1" do
    test "starts the message buffer process" do
      assert {:ok, pid} =
               MessageBuffer.start_link(
                 topic: @test_topic,
                 partition: @test_partition,
                 max_queue_size: 100
               )

      assert Process.alive?(pid)
    end

    test "registers with the correct name" do
      {:ok, _pid} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition
        )

      assert [{pid, _}] =
               Registry.lookup(
                 Kafkaesque.TopicRegistry,
                 {:message_buffer, @test_topic, @test_partition}
               )

      assert Process.alive?(pid)
    end
  end

  describe "enqueue/3" do
    setup do
      {:ok, _pid} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 10
        )

      :ok
    end

    test "enqueues messages successfully" do
      messages = create_test_records(3)
      assert {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, messages)
    end

    test "applies backpressure when queue is full" do
      # Fill the queue
      messages = create_test_records(10)
      assert {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, messages)

      # Try to add more - should fail
      more_messages = create_test_records(1)

      assert {:error, :queue_full} =
               MessageBuffer.enqueue(@test_topic, @test_partition, more_messages)
    end

    test "handles empty message list" do
      assert {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, [])
    end
  end

  describe "pull/3" do
    setup do
      {:ok, _pid} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      :ok
    end

    test "pulls messages when available" do
      messages = create_test_records(5)
      {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, messages)

      # Pull some messages
      assert {:ok, pulled} = MessageBuffer.pull(@test_topic, @test_partition, 3)
      assert length(pulled) == 3
      assert Enum.at(pulled, 0).key == "test_key_1"
    end

    test "returns empty list when no messages available" do
      assert {:ok, []} = MessageBuffer.pull(@test_topic, @test_partition, 5)
    end

    test "pulls all available messages if demand exceeds supply" do
      messages = create_test_records(3)
      {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, messages)

      assert {:ok, pulled} = MessageBuffer.pull(@test_topic, @test_partition, 10)
      assert length(pulled) == 3
    end

    test "maintains FIFO order" do
      # Enqueue in batches
      batch1 = create_test_records(2, "batch1")
      batch2 = create_test_records(2, "batch2")

      {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, batch1)
      {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, batch2)

      # Pull all messages
      {:ok, pulled} = MessageBuffer.pull(@test_topic, @test_partition, 10)

      # Check order
      assert Enum.at(pulled, 0).key == "batch1_key_1"
      assert Enum.at(pulled, 1).key == "batch1_key_2"
      assert Enum.at(pulled, 2).key == "batch2_key_1"
      assert Enum.at(pulled, 3).key == "batch2_key_2"
    end
  end

  describe "get_stats/2" do
    setup do
      {:ok, _pid} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 50
        )

      :ok
    end

    test "returns buffer statistics" do
      stats = MessageBuffer.get_stats(@test_topic, @test_partition)

      assert stats.queue_size == 0
      assert stats.max_queue_size == 50
      assert stats.has_waiting_demand == false
    end

    test "updates stats after enqueue" do
      messages = create_test_records(5)
      {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, messages)

      stats = MessageBuffer.get_stats(@test_topic, @test_partition)
      assert stats.queue_size == 5
    end

    test "updates stats after pull" do
      messages = create_test_records(5)
      {:ok, :enqueued} = MessageBuffer.enqueue(@test_topic, @test_partition, messages)
      {:ok, _} = MessageBuffer.pull(@test_topic, @test_partition, 2)

      stats = MessageBuffer.get_stats(@test_topic, @test_partition)
      assert stats.queue_size == 3
    end
  end

  describe "demand handling" do
    setup do
      {:ok, _pid} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      :ok
    end

    test "tracks waiting demand" do
      # First pull should return empty and store demand internally
      assert {:ok, []} = MessageBuffer.pull(@test_topic, @test_partition, 5)

      # Check stats shows waiting demand
      stats = MessageBuffer.get_stats(@test_topic, @test_partition)
      assert stats.has_waiting_demand == true

      # Second pull should overwrite the waiting demand
      assert {:ok, []} = MessageBuffer.pull(@test_topic, @test_partition, 10)

      stats = MessageBuffer.get_stats(@test_topic, @test_partition)
      assert stats.has_waiting_demand == true
    end
  end
end
