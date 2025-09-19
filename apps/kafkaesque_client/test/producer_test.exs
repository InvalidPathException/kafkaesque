defmodule KafkaesqueClient.ProducerTest do
  use ExUnit.Case, async: false

  alias KafkaesqueClient
  alias KafkaesqueClient.{Admin, Producer}
  alias KafkaesqueClient.Record.ProducerRecord

  setup_all do
    # Start necessary dependencies
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

    # Start gRPC server on test port
    case GRPC.Server.start_endpoint(KafkaesqueServer.GRPC.Endpoint, 50_053) do
      {:ok, _pid, port} ->
        IO.puts("Started gRPC server on port #{port}")
        on_exit(fn ->
          try do
            GRPC.Server.stop_endpoint(KafkaesqueServer.GRPC.Endpoint)
          rescue
            _ -> :ok
          end
        end)
        :ok
      {:error, {:already_started, _}} ->
        :ok
      error ->
        IO.puts("Warning: Could not start gRPC server: #{inspect(error)}")
    end

    Process.sleep(100)

    # Start client infrastructure with corrected naming
    children = [
      {Registry, keys: :unique, name: KafkaesqueClient.ConnectionRegistry},
      {DynamicSupervisor, name: KafkaesqueClient.ConnectionSupervisor, strategy: :one_for_one},
      {KafkaesqueClient.Connection.Pool, bootstrap_servers: ["localhost:50053"]}
    ]

    for child <- children do
      name = case child do
        {mod, opts} when is_list(opts) -> Keyword.get(opts, :name, mod)
        {mod, _} -> mod
      end

      case Process.whereis(name) do
        nil -> start_supervised!(child)
        _pid -> :ok
      end
    end

    :ok
  end

  setup do
    topic = "test-producer-topic-#{:rand.uniform(100_000)}"
    {:ok, admin} = KafkaesqueClient.create_admin(bootstrap_servers: ["localhost:50053"])
    {:ok, _} = Admin.create_topic(admin, topic, partitions: 1)
    {:ok, topic: topic}
  end

  describe "callback timeout mechanism" do
    test "cleans up expired callbacks after timeout period", %{topic: topic} do
      # Create producer with short callback timeout for testing
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 60_000,  # Very long linger to prevent auto-send
        callback_timeout_ms: 1000  # 1 second timeout
      })

      # Track callback execution
      test_pid = self()
      callback_executed = fn result ->
        send(test_pid, {:callback_executed, result})
      end

      # Send record with callback but don't flush
      record = ProducerRecord.new(topic, "key", "value")
      Producer.send(producer, record, callback_executed)

      # Wait for cleanup cycle (happens every 5 seconds, but callback should timeout after 1 second)
      Process.sleep(6_000)

      # Verify callback was executed with timeout error
      assert_receive {:callback_executed, {:error, :timeout}}, 1_000

      # Verify metrics show timeout
      metrics = Producer.metrics(producer)
      assert metrics.callbacks_timeout > 0

      Producer.close(producer)
    end

    test "executes timed-out callbacks with timeout error", %{topic: topic} do
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 60_000,
        callback_timeout_ms: 500
      })

      # Send multiple records with callbacks
      test_pid = self()

      for i <- 1..3 do
        record = ProducerRecord.new(topic, "key-#{i}", "value-#{i}")

        Producer.send(producer, record, fn result ->
          send(test_pid, {:callback, i, result})
        end)
      end

      # Wait for cleanup and timeout
      Process.sleep(6_000)

      # All callbacks should receive timeout error
      for i <- 1..3 do
        assert_receive {:callback, ^i, {:error, :timeout}}, 100
      end

      Producer.close(producer)
    end

    test "sync sends timeout and receive error response", %{topic: topic} do
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 60_000,
        callback_timeout_ms: 1000
      })

      # Start async task for sync send
      record = ProducerRecord.new(topic, "key", "value")

      task = Task.async(fn ->
        Producer.send_sync(producer, record, 10_000)
      end)

      # Wait for timeout
      Process.sleep(6_000)

      # Task should get timeout error
      result = Task.await(task)
      assert {:error, :timeout} = result

      Producer.close(producer)
    end

    test "callbacks are properly removed after successful batch send", %{topic: topic} do
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 100,
        linger_ms: 10
      })

      test_pid = self()

      # Send and flush immediately
      record = ProducerRecord.new(topic, "key", "value")
      Producer.send(producer, record, fn result ->
        send(test_pid, {:callback, result})
      end)

      Producer.flush(producer)

      # Callback should execute with success
      assert_receive {:callback, {:ok, metadata}}, 1_000
      assert metadata.topic == topic

      # Wait for cleanup cycle
      Process.sleep(5_100)

      # No timeout callbacks should have been triggered
      metrics = Producer.metrics(producer)
      assert metrics.callbacks_timeout == 0

      Producer.close(producer)
    end
  end

  describe "async flush" do
    test "async flush returns immediately while batch sends in background", %{topic: topic} do
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 60_000
      })

      test_pid = self()

      # Send records
      for i <- 1..5 do
        record = ProducerRecord.new(topic, "key-#{i}", "value-#{i}")
        Producer.send(producer, record, fn {:ok, _metadata} ->
          send(test_pid, {:sent, i})
        end)
      end

      # Async flush should return immediately
      start_time = System.monotonic_time(:millisecond)
      :ok = Producer.flush_async(producer)
      duration = System.monotonic_time(:millisecond) - start_time

      # Should return in less than 10ms (essentially immediate)
      assert duration < 10

      # Records should be sent in background
      for i <- 1..5 do
        assert_receive {:sent, ^i}, 5_000
      end

      Producer.close(producer)
    end

    test "multiple async flushes can execute concurrently", %{topic: topic} do
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 60_000
      })

      test_pid = self()

      # First batch
      for i <- 1..3 do
        record = ProducerRecord.new(topic, "batch1-#{i}", "value-#{i}")
        Producer.send(producer, record, fn {:ok, _} ->
          send(test_pid, {:batch1, i})
        end)
      end

      # First async flush
      Producer.flush_async(producer)

      # Second batch immediately after
      for i <- 1..3 do
        record = ProducerRecord.new(topic, "batch2-#{i}", "value-#{i}")
        Producer.send(producer, record, fn {:ok, _} ->
          send(test_pid, {:batch2, i})
        end)
      end

      # Second async flush
      Producer.flush_async(producer)

      # Both batches should complete
      for i <- 1..3 do
        assert_receive {:batch1, ^i}, 5_000
        assert_receive {:batch2, ^i}, 5_000
      end

      Producer.close(producer)
    end

    test "async flush with empty batch does nothing", %{topic: _topic} do
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 100
      })

      # Flush empty batch should be safe
      :ok = Producer.flush_async(producer)

      # Verify no errors occur
      Process.sleep(100)

      metrics = Producer.metrics(producer)
      assert metrics.batches_sent == 0
      assert metrics.records_sent == 0

      Producer.close(producer)
    end

    test "async flush followed by sync flush works correctly", %{topic: topic} do
      {:ok, producer} = Producer.start_link(%{
        bootstrap_servers: ["localhost:50053"],
        acks: :leader,
        batch_size: 1024,
        linger_ms: 60_000
      })

      test_pid = self()

      # First batch with async flush
      record1 = ProducerRecord.new(topic, "async-key", "async-value")
      Producer.send(producer, record1, fn {:ok, _} ->
        send(test_pid, :async_sent)
      end)
      Producer.flush_async(producer)

      # Second batch with sync flush
      record2 = ProducerRecord.new(topic, "sync-key", "sync-value")
      Producer.send(producer, record2, fn {:ok, _} ->
        send(test_pid, :sync_sent)
      end)
      Producer.flush(producer)

      # Both should complete
      assert_receive :async_sent, 5_000
      assert_receive :sync_sent, 5_000

      Producer.close(producer)
    end
  end
end
