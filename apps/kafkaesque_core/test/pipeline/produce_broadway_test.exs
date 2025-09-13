defmodule Kafkaesque.Pipeline.ProduceBroadwayTest do
  use ExUnit.Case, async: false

  alias Kafkaesque.Pipeline.MessageBuffer
  alias Kafkaesque.Pipeline.ProduceBroadway
  alias Kafkaesque.Storage.SingleFile

  @test_topic "broadway_test"
  @test_partition 0
  @test_dir "./test_broadway"

  setup do
    # Clean up test directory
    File.rm_rf!(@test_dir)
    File.mkdir_p!(@test_dir)

    # Start required processes
    case Registry.start_link(keys: :unique, name: Kafkaesque.TopicRegistry) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    # Start storage
    {:ok, _} =
      SingleFile.start_link(
        topic: @test_topic,
        partition: @test_partition,
        data_dir: @test_dir
      )

    on_exit(fn ->
      File.rm_rf!(@test_dir)
    end)

    :ok
  end

  describe "start_link/1" do
    test "starts the Broadway pipeline" do
      # Start MessageBuffer first
      {:ok, _buffer} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      assert {:ok, pid} =
               ProduceBroadway.start_link(
                 topic: @test_topic,
                 partition: @test_partition,
                 batch_size: 10,
                 batch_timeout: 1
               )

      assert Process.alive?(pid)
    end
  end

  describe "produce/4" do
    setup do
      # Start MessageBuffer first (Broadway depends on it)
      {:ok, _buffer} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      case ProduceBroadway.start_link(
             topic: @test_topic,
             partition: @test_partition,
             batch_size: 5,
             batch_timeout: 1
           ) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end

      :ok
    end

    test "produces single message" do
      message = %{
        key: "test_key",
        value: "test_value",
        headers: []
      }

      assert {:ok, result} =
               ProduceBroadway.produce(
                 @test_topic,
                 @test_partition,
                 [message],
                 acks: :leader
               )

      assert result.topic == @test_topic
      assert result.partition == @test_partition
      assert result.count == 1
    end

    test "produces batch of messages" do
      messages =
        Enum.map(1..10, fn i ->
          %{
            key: "key_#{i}",
            value: "value_#{i}"
          }
        end)

      assert {:ok, result} =
               ProduceBroadway.produce(
                 @test_topic,
                 @test_partition,
                 messages,
                 acks: :leader
               )

      assert result.count == 10
    end

    test "handles no-ack mode" do
      message = %{
        key: "test_key",
        value: "test_value"
      }

      # With acks: :none, should still work but potentially return faster
      assert {:ok, _result} =
               ProduceBroadway.produce(
                 @test_topic,
                 @test_partition,
                 [message],
                 acks: :none
               )
    end

    test "batches messages based on configuration" do
      # Send messages in rapid succession
      # They should be batched together
      messages =
        Enum.map(1..3, fn i ->
          %{key: "key_#{i}", value: "value_#{i}"}
        end)

      # Send all at once - should be in same batch
      assert {:ok, result} =
               ProduceBroadway.produce(
                 @test_topic,
                 @test_partition,
                 messages
               )

      assert result.count == 3
      assert result.base_offset == 0
    end
  end

  describe "get_metrics/2" do
    setup do
      # Start MessageBuffer first
      {:ok, _buffer} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      {:ok, _pid} =
        ProduceBroadway.start_link(
          topic: @test_topic,
          partition: @test_partition
        )

      :ok
    end

    test "returns pipeline metrics" do
      metrics = ProduceBroadway.get_metrics(@test_topic, @test_partition)

      assert is_map(metrics)
      assert Map.has_key?(metrics, :queue_length)
      assert Map.has_key?(metrics, :memory)
      assert metrics.queue_length >= 0
      assert metrics.memory >= 0
    end
  end

  describe "drain_and_stop/2" do
    test "gracefully stops the pipeline" do
      # Start MessageBuffer first
      {:ok, _buffer} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      # Handle case where Broadway might already be started
      pid =
        case ProduceBroadway.start_link(
               topic: @test_topic,
               partition: @test_partition
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      assert Process.alive?(pid)

      :ok = ProduceBroadway.drain_and_stop(@test_topic, @test_partition)

      # Monitor the process to detect when it stops
      ref = Process.monitor(pid)

      # Should receive DOWN message when process stops
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5000

      # Process should no longer be alive
      refute Process.alive?(pid)
    end

    test "processes pending messages before stopping" do
      # Start MessageBuffer first
      {:ok, _buffer} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      # Handle case where Broadway might already be started
      _pid =
        case ProduceBroadway.start_link(
               topic: @test_topic,
               partition: @test_partition,
               # Large batch to ensure messages are pending
               batch_size: 100,
               # Long timeout
               batch_timeout: 10
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      # Send messages
      messages =
        Enum.map(1..5, fn i ->
          %{key: "key_#{i}", value: "value_#{i}"}
        end)

      # Produce messages - they should be queued
      assert {:ok, _result} = ProduceBroadway.produce(@test_topic, @test_partition, messages)

      # Now drain and stop - should process pending messages
      :ok = ProduceBroadway.drain_and_stop(@test_topic, @test_partition)
    end
  end

  describe "error handling" do
    setup do
      # Start MessageBuffer first
      {:ok, _buffer} =
        MessageBuffer.start_link(
          topic: @test_topic,
          partition: @test_partition,
          max_queue_size: 100
        )

      case ProduceBroadway.start_link(
             topic: @test_topic,
             partition: @test_partition,
             batch_size: 5,
             batch_timeout: 1
           ) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end

      :ok
    end

    test "handles malformed messages gracefully" do
      # Messages with various issues
      messages = [
        # No key or value
        %{},
        # Nil values
        %{key: nil, value: nil},
        # Empty strings
        %{key: "", value: ""}
      ]

      # Should handle without crashing
      assert {:ok, _result} =
               ProduceBroadway.produce(
                 @test_topic,
                 @test_partition,
                 messages
               )
    end
  end
end
