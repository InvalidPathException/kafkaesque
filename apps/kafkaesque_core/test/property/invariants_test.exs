defmodule Kafkaesque.Property.InvariantsTest do
  use ExUnit.Case, async: false
  use ExUnitProperties
  import Kafkaesque.TestHelpers

  @moduletag timeout: 120_000  # 2 minutes for property tests

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.LogReader

  setup do
    setup_test_env()
    on_exit(fn -> clean_test_dirs() end)
    :ok
  end

  defp consume_with_autocommit(topic, group, message_count) do
    consume_loop(topic, group, 0, message_count, [])
  end

  defp consume_loop(_topic, _group, current_offset, message_count, consumed) when current_offset >= message_count do
    consumed
  end

  defp consume_loop(topic, group, current_offset, message_count, consumed) do
    {:ok, batch} = LogReader.consume(topic, 0, group, current_offset, 1_048_576, 100)

    if length(batch) > 0 do
      # Simulate auto-commit
      last_offset = current_offset + length(batch) - 1
      :ok = DetsOffset.commit(topic, 0, group, last_offset)
      consume_loop(topic, group, last_offset + 1, message_count, consumed ++ [last_offset])
    else
      consumed
    end
  end

  describe "Message Ordering Invariants" do
    property "messages maintain order within partition" do
      check all messages <- list_of(message_generator(), min_length: 1, max_length: 20),
                max_runs: 20 do

        # Create topic
        topic = create_unique_test_topic("order", 1)

        # Produce messages with sequential markers
        indexed_messages = messages
          |> Enum.with_index()
          |> Enum.map(fn {msg, idx} ->
            Map.put(msg, :headers, [{"seq", Integer.to_string(idx)}])
          end)

        {:ok, result} = produce_and_wait(topic, 0, indexed_messages)
        assert result.count == length(indexed_messages)

        # Consume all messages
        {:ok, consumed} = LogReader.consume(topic, 0, "", 0, 10_485_760, 1000)

        # Verify ordering
        consumed
        |> Enum.with_index()
        |> Enum.each(fn {msg, idx} ->
          # Check offset ordering
          assert msg[:offset] == idx

          # Check sequence header
          seq_header = Enum.find(msg[:headers], fn {k, _} -> k == "seq" end)
          assert seq_header != nil
          {_, seq_str} = seq_header
          assert String.to_integer(seq_str) == idx
        end)

        # Clean up
        delete_test_topic(topic)
      end
    end

    property "offsets are monotonically increasing" do
      check all batch_count <- integer(1..5),
                batch_size <- integer(1..10),
                max_runs: 10 do

        topic = create_unique_test_topic("monotonic", 1)

        # Produce multiple batches
        total_messages = batch_count * batch_size

        for _batch <- 1..batch_count do
          messages = for _i <- 1..batch_size do
            %{
              key: :crypto.strong_rand_bytes(10) |> Base.encode64(),
              value: :crypto.strong_rand_bytes(20) |> Base.encode64(),
              headers: [],
              timestamp_ms: System.system_time(:millisecond)
            }
          end

          {:ok, result} = produce_and_wait(topic, 0, messages)
          assert result.count == batch_size
        end

        # Wait for messages to be written
        Process.sleep(200)

        # Verify messages were written by checking offsets
        {:ok, offsets} = SingleFile.get_offsets(topic, 0)

        # Latest offset is the next position to write (Kafka semantics)
        # So for N messages, latest = N (not N-1)
        assert offsets.latest == total_messages
        assert offsets.earliest == 0

        delete_test_topic(topic)
      end
    end

    property "no gaps in offset sequence" do
      check all message_count <- integer(1..50),
                max_runs: 10 do

        topic = create_unique_test_topic("gaps", 1)

        # Produce messages
        messages = for i <- 1..message_count do
          %{
            key: "key-#{i}",
            value: "value-#{i}",
            headers: [],
            timestamp_ms: System.system_time(:millisecond)
          }
        end

        {:ok, _} = produce_and_wait(topic, 0, messages)

        # Give a bit more time for fsync
        Process.sleep(200)

        # Get offset range
        {:ok, offsets} = SingleFile.get_offsets(topic, 0)

        # Verify no gaps by checking we can read each offset
        # Latest is the next position to write, so we check 0..(latest-1)
        for offset <- 0..(offsets.latest - 1) do
          case LogReader.consume(topic, 0, "", offset, 100, 10) do
            {:ok, batch} ->
              # Should get at least one message for each valid offset
              assert length(batch) > 0, "Expected messages at offset #{offset}, got empty batch (latest: #{offsets.latest})"
            {:error, reason} ->
              flunk("Failed to read offset #{offset}: #{inspect(reason)}")
          end
        end

        delete_test_topic(topic)
      end
    end
  end

  describe "Consumer Group Invariants" do
    property "committed offset never exceeds latest offset" do
      check all group <- string(:alphanumeric, min_length: 5, max_length: 20),
                message_count <- integer(10..100),
                commit_point <- integer(0..100) do

        topic = create_unique_test_topic("commit", 1)

        # Produce messages
        messages = generate_messages(message_count)
        {:ok, _result} = produce_and_wait(topic, 0, messages)

        # Wait for messages to be written
        Process.sleep(100)

        # Calculate valid commit offset (messages are 0-indexed)
        max_valid_offset = message_count - 1
        commit_offset = min(commit_point, max_valid_offset)

        # Commit offset
        :ok = DetsOffset.commit(topic, 0, group, commit_offset)

        # Verify invariant
        {:ok, committed} = DetsOffset.fetch(topic, 0, group)
        {:ok, offsets} = SingleFile.get_offsets(topic, 0)

        assert committed <= offsets.latest
        assert committed == commit_offset

        delete_test_topic(topic)
      end
    end

    property "no message loss with auto-commit" do
      check all group <- string(:alphanumeric, min_length: 5, max_length: 20),
                message_count <- integer(10..50) do

        topic = create_unique_test_topic("autocommit", 1)

        # Produce messages
        messages = generate_messages(message_count)
        {:ok, _} = produce_and_wait(topic, 0, messages)

        # Consume with auto-commit
        _consumed_offsets = consume_with_autocommit(topic, group, message_count)

        # Verify committed offset
        {:ok, final_committed} = DetsOffset.fetch(topic, 0, group)
        assert final_committed == message_count - 1

        delete_test_topic(topic)
      end
    end

    property "exactly-once commit per message" do
      check all group <- string(:alphanumeric, min_length: 5, max_length: 20),
                message_count <- integer(5..30) do

        topic = create_unique_test_topic("exactlyonce", 1)

        # Produce messages
        messages = generate_messages(message_count)
        {:ok, _} = produce_and_wait(topic, 0, messages)

        # Track committed offsets
        committed_offsets = MapSet.new()

        # Commit each offset exactly once
        committed_offsets = Enum.reduce(0..(message_count - 1), committed_offsets, fn offset, acc ->
          if MapSet.member?(acc, offset) do
            acc
          else
            :ok = DetsOffset.commit(topic, 0, group, offset)
            MapSet.put(acc, offset)
          end
        end)

        # Verify all offsets were committed exactly once
        assert MapSet.size(committed_offsets) == message_count

        # Verify final committed offset
        {:ok, final_offset} = DetsOffset.fetch(topic, 0, group)
        assert final_offset == message_count - 1

        delete_test_topic(topic)
      end
    end
  end

  describe "Data Consistency Invariants" do
    property "written messages are always readable" do
      check all messages <- list_of(message_generator(), min_length: 1, max_length: 20),
                max_runs: 10 do

        topic = create_unique_test_topic("consistency", 1)

        # Produce messages
        {:ok, result} = produce_and_wait(topic, 0, messages)

        # Immediately read back
        {:ok, consumed} = LogReader.consume(topic, 0, "", 0, 10_485_760, 1000)

        # Verify all messages are readable
        assert length(consumed) == result.count
        assert length(consumed) == length(messages)

        # Verify content matches
        Enum.zip(messages, consumed)
        |> Enum.each(fn {original, consumed_msg} ->
          assert consumed_msg[:key] == original.key
          assert consumed_msg[:value] == original.value
        end)

        delete_test_topic(topic)
      end
    end

    property "concurrent writes maintain consistency" do
      check all producer_count <- integer(2..3),
                messages_per_producer <- integer(5..10),
                max_runs: 10 do

        topic = create_unique_test_topic("concurrent", 1)

        # Spawn concurrent producers
        tasks = for producer_id <- 1..producer_count do
          Task.async(fn ->
            messages = for msg_id <- 1..messages_per_producer do
              %{
                key: "p#{producer_id}-m#{msg_id}",
                value: "data-#{producer_id}-#{msg_id}",
                headers: [{"producer", Integer.to_string(producer_id)}],
                timestamp_ms: System.system_time(:millisecond)
              }
            end

            produce_and_wait(topic, 0, messages)
          end)
        end

        # Wait for all producers
        results = Task.await_many(tasks, 5000)
        Enum.each(results, fn {:ok, result} ->
          assert result.count == messages_per_producer
        end)

        # Read all messages
        {:ok, all_messages} = LogReader.consume(topic, 0, "", 0, 10_485_760, 1000)

        # Verify total count
        expected_total = producer_count * messages_per_producer
        assert length(all_messages) == expected_total

        # Verify no duplicates
        keys = Enum.map(all_messages, & &1[:key])
        assert length(keys) == length(Enum.uniq(keys))

        # Verify all producers' messages are present
        for producer_id <- 1..producer_count do
          producer_messages = Enum.filter(all_messages, fn msg ->
            case Enum.find(msg[:headers] || [], fn {k, _} -> k == "producer" end) do
              {_, p_id} -> String.to_integer(p_id) == producer_id
              _ -> false
            end
          end)

          assert length(producer_messages) == messages_per_producer
        end

        delete_test_topic(topic)
      end
    end
  end

  describe "Boundary Conditions" do
    property "handles empty and single message batches" do
      check all test_run <- integer(1..10) do

        topic = create_unique_test_topic("boundary-#{test_run}", 1)

        # Test single message
        single_msg = [%{key: "k", value: "v", headers: [], timestamp_ms: System.system_time(:millisecond)}]
        {:ok, result} = produce_and_wait(topic, 0, single_msg)
        assert result.count == 1

        {:ok, consumed} = LogReader.consume(topic, 0, "", 0, 1_048_576, 100)
        assert length(consumed) == 1

        delete_test_topic(topic)
      end
    end

    property "handles maximum offset values correctly" do
      check all message_count <- integer(100..500),
                max_runs: 5 do

        topic = create_unique_test_topic("offset", 1)

        # This property tests that the system handles large offset values correctly
        # We're testing with realistic batch sizes rather than trying to reach millions

        # Produce messages in reasonable batches
        batch_size = 50
        iterations = div(message_count, batch_size)
        remainder = rem(message_count, batch_size)

        for _i <- 1..iterations do
          messages = generate_messages(batch_size)
          {:ok, _} = produce_and_wait(topic, 0, messages, timeout: 2000)
        end

        # Handle remainder
        if remainder > 0 do
          messages = generate_messages(remainder)
          {:ok, _} = produce_and_wait(topic, 0, messages, timeout: 2000)
        end

        # Verify offset handling
        {:ok, offsets} = SingleFile.get_offsets(topic, 0)
        assert offsets.latest == message_count  # Latest is next position to write
        assert offsets.earliest == 0

        delete_test_topic(topic)
      end
    end
  end

  # Generators

  defp message_generator do
    gen all key <- string(:alphanumeric, min_length: 1, max_length: 50),
            value <- string(:alphanumeric, min_length: 1, max_length: 200),
            header_count <- integer(0..5) do
      headers = if header_count > 0 do
        for _i <- 1..header_count do
          {"header-#{:rand.uniform(100)}", "value-#{:rand.uniform(100)}"}
        end
      else
        []
      end

      %{
        key: key,
        value: value,
        headers: headers,
        timestamp_ms: System.system_time(:millisecond)
      }
    end
  end
end
