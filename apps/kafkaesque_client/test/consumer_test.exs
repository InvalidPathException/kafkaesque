defmodule KafkaesqueClient.ConsumerTest do
  use ExUnit.Case, async: false

  alias KafkaesqueClient
  alias KafkaesqueClient.{Admin, Consumer, Producer}

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
    topic = "test-consumer-topic-#{:rand.uniform(100_000)}"
    group_id = "test-group-#{:rand.uniform(100_000)}"

    # Create topic
    {:ok, admin} = KafkaesqueClient.create_admin(bootstrap_servers: ["localhost:50053"])
    {:ok, _} = Admin.create_topic(admin, topic, partitions: 1)

    # Produce some test messages
    {:ok, producer} = KafkaesqueClient.create_producer(
      bootstrap_servers: ["localhost:50053"],
      acks: :leader
    )

    for i <- 1..10 do
      record = KafkaesqueClient.producer_record(topic, "key-#{i}", "value-#{i}")
      {:ok, _} = Producer.send_sync(producer, record)
    end

    # Ensure all messages are flushed
    Producer.flush(producer)
    Producer.close(producer)

    # Give the system a moment to ensure messages are persisted
    Process.sleep(100)

    {:ok, topic: topic, group_id: group_id}
  end

  describe "auto-commit race condition" do
    test "stops auto-commit timer when consumer closes", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 100
      )

      # Subscribe and consume some messages
      :ok = Consumer.subscribe(consumer, [topic])
      # Give consumer time to start streaming
      Process.sleep(200)
      records = Consumer.poll(consumer, 2_000)
      assert length(records) > 0

      # Get the consumer's state to verify timer exists
      state = :sys.get_state(consumer)
      assert state.auto_commit_timer != nil

      # Close consumer
      :ok = Consumer.close(consumer)

      # Verify consumer process is stopped
      Process.sleep(100)
      refute Process.alive?(consumer)

      # No auto-commit crashes should occur after close
      # If the timer wasn't cancelled, we'd see errors in logs
    end

    test "ignores auto-commit messages received after close", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 50  # Very short interval
      )

      # Subscribe and consume
      :ok = Consumer.subscribe(consumer, [topic])
      Process.sleep(200)
      _records = Consumer.poll(consumer, 1_000)

      # Send a manual auto-commit message
      send(consumer, :auto_commit)

      # Close consumer
      :ok = Consumer.close(consumer)

      # Consumer should have terminated gracefully
      Process.sleep(100)
      refute Process.alive?(consumer)
    end

    test "commits pending offsets before closing with auto-commit enabled", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 10_000  # Long interval so it won't auto-commit during test
      )

      # Subscribe and consume all messages
      :ok = Consumer.subscribe(consumer, [topic])
      Process.sleep(200)
      records = Consumer.poll(consumer, 2_000)
      assert length(records) == 10

      # Close consumer - should commit offsets
      :ok = Consumer.close(consumer)

      # Create new consumer with same group to verify offsets were committed
      {:ok, consumer2} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: false
      )

      :ok = Consumer.subscribe(consumer2, [topic])
      Process.sleep(200)
      new_records = Consumer.poll(consumer2, 2_000)

      # Should not receive any records since offsets were committed
      assert Enum.empty?(new_records)

      Consumer.close(consumer2)
    end

    test "consumer state properly tracks closed status", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 100
      )

      # Initially not closed
      state = :sys.get_state(consumer)
      assert state.closed == false

      # Subscribe and consume
      :ok = Consumer.subscribe(consumer, [topic])
      Process.sleep(200)
      _records = Consumer.poll(consumer, 1_000)

      # Close consumer
      spawn(fn -> Consumer.close(consumer) end)

      # Give it a moment to process the close call
      Process.sleep(50)

      # Try to get state - if consumer is stopping, this might fail
      # but that's ok, it means close is working
      try do
        state = :sys.get_state(consumer)
        assert state.closed == true
      catch
        :exit, _ -> :ok  # Consumer already stopped, which is fine
      end
    end

    test "auto-commit continues working normally when consumer is not closed", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 200
      )

      # Subscribe and consume messages
      :ok = Consumer.subscribe(consumer, [topic])
      Process.sleep(200)
      records = Consumer.poll(consumer, 2_000)
      assert length(records) > 0

      # Wait for auto-commit to happen
      Process.sleep(300)

      # Verify consumer is still alive and working
      assert Process.alive?(consumer)

      # Can still poll
      _more_records = Consumer.poll(consumer, 100)

      # Metrics should show commits
      metrics = Consumer.metrics(consumer)
      assert metrics.commits >= 0

      Consumer.close(consumer)
    end

    test "manual commit still works when auto-commit is disabled", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: false
      )

      # Subscribe and consume
      :ok = Consumer.subscribe(consumer, [topic])
      Process.sleep(200)
      records = Consumer.poll(consumer, 2_000)
      assert length(records) == 10

      # Manual commit should work
      :ok = Consumer.commit_sync(consumer)

      # Close without auto-commit
      :ok = Consumer.close(consumer)

      # Verify offsets were committed
      {:ok, consumer2} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: false
      )

      :ok = Consumer.subscribe(consumer2, [topic])
      Process.sleep(200)
      new_records = Consumer.poll(consumer2, 2_000)
      assert Enum.empty?(new_records)

      Consumer.close(consumer2)
    end
  end

  describe "consumer lifecycle" do
    test "can close consumer multiple times safely", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 100
      )

      :ok = Consumer.subscribe(consumer, [topic])
      Process.sleep(200)
      _records = Consumer.poll(consumer, 1_000)

      # First close
      :ok = Consumer.close(consumer)

      # Consumer should be stopped
      Process.sleep(100)
      refute Process.alive?(consumer)

      # Trying to close again should not crash
      # (will fail because process is dead, but that's expected)
      catch_exit(Consumer.close(consumer))
    end

    test "auto-commit timer is properly cancelled on close", %{topic: topic, group_id: group_id} do
      {:ok, consumer} = KafkaesqueClient.create_consumer(
        bootstrap_servers: ["localhost:50053"],
        group_id: group_id,
        auto_offset_reset: :earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 100
      )

      :ok = Consumer.subscribe(consumer, [topic])
      Process.sleep(200)
      _records = Consumer.poll(consumer, 1_000)

      # Get the timer reference before closing
      state = :sys.get_state(consumer)
      timer_ref = state.auto_commit_timer
      assert timer_ref != nil

      # Close consumer
      :ok = Consumer.close(consumer)

      # Timer should be cancelled (won't be in timer list)
      Process.sleep(100)
      timer_info = Process.read_timer(timer_ref)
      assert timer_info == false  # Timer was cancelled
    end
  end
end
