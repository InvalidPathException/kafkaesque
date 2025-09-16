defmodule Kafkaesque.TestHelpers do
  @moduledoc """
  Common test helpers for Kafkaesque tests.
  """

  import ExUnit.Assertions

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Topic.LogReader
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  @grpc_port 50_052  # Use different port for tests
  @http_port 4001

  def grpc_port, do: @grpc_port
  def http_port, do: @http_port

  @doc """
  Set up a clean test environment for each test.
  """
  def setup_test_env do
    # Clean directories more thoroughly
    clean_test_dirs()

    # Also clean project-local test directories if they exist
    File.rm_rf!("test_data")
    File.rm_rf!("test_offsets")

    # Create fresh directories
    File.mkdir_p!("/tmp/kafkaesque_test/data")
    File.mkdir_p!("/tmp/kafkaesque_test/offsets")

    # Reset ETS tables if they exist
    reset_ets_tables()

    # Kill any lingering test processes
    kill_lingering_processes()
  end

  @doc """
  Clean up test directories.
  """
  def clean_test_dirs do
    # Remove test directory completely
    File.rm_rf!("/tmp/kafkaesque_test")

    # Also clean project-local test directories
    File.rm_rf!("test_data")
    File.rm_rf!("test_offsets")
  end

  defp kill_lingering_processes do
    # Kill any lingering topic processes from previous test runs
    # Only if the registry exists
    Registry.select(Kafkaesque.TopicRegistry, [{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.each(fn {_key, pid} ->
      if Process.alive?(pid) do
        Process.exit(pid, :kill)
      end
    end)

    # Small delay to ensure processes are dead
    Process.sleep(10)
  rescue
    ArgumentError -> :ok  # Registry doesn't exist yet
  end

  @doc """
  Reset ETS tables used in the system.
  """
  def reset_ets_tables do
    # Reset telemetry ETS table if it exists
    case :ets.whereis(:kafkaesque_telemetry) do
      :undefined -> :ok
      _ -> :ets.delete_all_objects(:kafkaesque_telemetry)
    end
  end

  @doc """
  Create a test topic with optional number of partitions.
  """
  def create_test_topic(name, partitions \\ 1) do
    TopicSupervisor.create_topic(name, partitions)
  end

  @doc """
  Create a test topic with a unique suffix to avoid conflicts.
  """
  def create_unique_test_topic(base_name, partitions \\ 1) do
    # Use timestamp and random suffix for better uniqueness
    timestamp = System.system_time(:microsecond)
    suffix = :rand.uniform(9999)
    name = "#{base_name}-#{timestamp}-#{suffix}"

    # Ensure any old topic with same name is cleaned up first
    clean_topic_if_exists(name)

    # Retry topic creation with exponential backoff
    {:ok, _name} = create_topic_with_retry(name, partitions, 3)
    name
  end

  defp clean_topic_if_exists(name) do
    case Registry.lookup(Kafkaesque.TopicRegistry, name) do
      [{pid, _}] ->
        Process.exit(pid, :kill)
        wait_until(fn -> !Process.alive?(pid) end, 1000)
        Process.sleep(100)
        # Clean up files
        File.rm_rf!("/tmp/kafkaesque_test/data/#{name}")
        File.rm_rf!("/tmp/kafkaesque_test/offsets/#{name}")
      _ ->
        # Still clean up files if they exist
        File.rm_rf!("/tmp/kafkaesque_test/data/#{name}")
        File.rm_rf!("/tmp/kafkaesque_test/offsets/#{name}")
    end
  end

  defp create_topic_with_retry(name, partitions, retries) do
    case TopicSupervisor.create_topic(name, partitions) do
      {:ok, pid} ->
        {:ok, pid}
      {:error, {:already_started, _}} when retries > 0 ->
        # Topic already exists, try to clean it up first
        delete_test_topic(name)
        Process.sleep(200)
        # Try again with same name
        create_topic_with_retry(name, partitions, retries - 1)
      {:error, _reason} when retries > 0 ->
        # Other error, wait and retry
        Process.sleep(100 * (4 - retries))
        create_topic_with_retry(name, partitions, retries - 1)
      {:error, reason} ->
        raise "Failed to create topic #{name} after retries: #{inspect(reason)}"
    end
  end

  @doc """
  Recreate a topic after it was killed. Handles already_started errors.
  """
  def recreate_test_topic(name, partitions \\ 1) do
    # First try to create normally
    case TopicSupervisor.create_topic(name, partitions) do
      {:ok, pid} ->
        ensure_topic_ready(name, 0)
        {:ok, pid}
      {:error, {:already_started, pid}} ->
        # Topic exists, verify it's ready
        ensure_topic_ready(name, 0)
        {:ok, pid}
      {:error, _reason} ->
        # Something went wrong, clean up and retry once
        clean_topic_if_exists(name)
        Process.sleep(200)
        case TopicSupervisor.create_topic(name, partitions) do
          {:ok, pid} ->
            ensure_topic_ready(name, 0)
            {:ok, pid}
          error -> error
        end
    end
  end

  @doc """
  Delete a test topic.
  """
  def delete_test_topic(name) do
    # Use TopicSupervisor to properly delete the topic
    try do
      TopicSupervisor.delete_topic(name)
      # Small delay to ensure cleanup completes
      Process.sleep(50)
    rescue
      # If supervisor isn't started or topic doesn't exist, that's OK
      _ -> :ok
    end

    # Also clean up files to be sure
    File.rm_rf!("/tmp/kafkaesque_test/data/#{name}")
    File.rm_rf!("/tmp/kafkaesque_test/offsets/#{name}")
  end

  @doc """
  Generate test messages.
  """
  def generate_messages(count, opts \\ []) do
    key_prefix = Keyword.get(opts, :key_prefix, "key")
    value_prefix = Keyword.get(opts, :value_prefix, "value")

    Enum.map(1..count, fn i ->
      %{
        key: "#{key_prefix}-#{i}",
        value: "#{value_prefix}-#{i}",
        headers: [{"header1", "value1"}],
        timestamp_ms: System.system_time(:millisecond)
      }
    end)
  end

  @doc """
  Produce messages directly to a topic/partition.
  """
  def produce_messages(topic, partition, messages, opts \\ []) do
    Producer.produce(topic, partition, messages, opts)
  end

  @doc """
  Wait for a condition to be true, with timeout.
  """
  def wait_until(condition_fn, timeout \\ 5000, check_interval \\ 100) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(condition_fn, deadline, check_interval)
  end

  defp do_wait_until(condition_fn, deadline, check_interval) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      if condition_fn.() do
        :ok
      else
        Process.sleep(check_interval)
        do_wait_until(condition_fn, deadline, check_interval)
      end
    end
  end

  @doc """
  Produce messages and wait for them to be written to storage.
  Returns {:ok, result} with actual offsets once written.
  """
  def produce_and_wait(topic, partition, messages, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 10_000)  # Increased default timeout

    # Ensure topic is fully initialized first
    ensure_topic_ready(topic, partition)

    # Get initial offset
    initial_offsets = case SingleFile.get_offsets(topic, partition) do
      {:ok, offsets} -> offsets
      _ -> %{latest: -1, earliest: 0}
    end

    # Produce messages
    case Producer.produce(topic, partition, messages) do
      {:ok, _produce_result} ->
        expected_count = length(messages)
        expected_offset = initial_offsets.latest + expected_count

        # Wait for messages to be written with exponential backoff
        wait_result = wait_until_with_backoff(
          fn ->
            case SingleFile.get_offsets(topic, partition) do
              {:ok, offsets} -> offsets.latest >= expected_offset
              _ -> false
            end
          end,
          timeout,
          50,  # Start with 50ms
          500  # Max 500ms between checks
        )

        case wait_result do
          :ok ->
            # Double-check by waiting a bit more for file sync
            Process.sleep(100)

            # Get actual offsets after write
            {:ok, final_offsets} = SingleFile.get_offsets(topic, partition)
            base_offset = initial_offsets.latest + 1

            {:ok, %{
              topic: topic,
              partition: partition,
              base_offset: base_offset,
              count: expected_count,
              latest_offset: final_offsets.latest
            }}

          {:error, :timeout} ->
            # Get current state for debugging
            current = case SingleFile.get_offsets(topic, partition) do
              {:ok, o} -> o.latest
              _ -> -999
            end
            {:error, {:write_timeout, expected: expected_offset, got: current}}
        end

      error ->
        error
    end
  end

  defp ensure_topic_ready(topic, partition) do
    # Wait for all components to be registered with correct key format
    components = [
      {:storage, topic, partition},
      {:producer, topic, partition},
      {:batching_consumer, topic, partition}
    ]

    Enum.each(components, fn key ->
      wait_until(fn ->
        case Registry.lookup(Kafkaesque.TopicRegistry, key) do
          [{_pid, _}] -> true
          _ -> false
        end
      end, 2000)
    end)

    # Give components time to initialize
    Process.sleep(50)
  end

  defp wait_until_with_backoff(condition_fn, timeout, interval, max_interval) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_with_backoff(condition_fn, deadline, interval, max_interval)
  end

  defp do_wait_with_backoff(condition_fn, deadline, interval, max_interval) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      if condition_fn.() do
        :ok
      else
        Process.sleep(interval)
        next_interval = min(interval * 2, max_interval)
        do_wait_with_backoff(condition_fn, deadline, next_interval, max_interval)
      end
    end
  end

  @doc """
  Wait for a specific number of messages to be written to storage.
  """
  def wait_for_messages(topic, partition, expected_count, timeout \\ 5000) do
    wait_until(
      fn ->
        case SingleFile.get_offsets(topic, partition) do
          {:ok, offsets} ->
            # latest is the next position to write
            # So for N messages, latest should be N
            offsets.latest >= expected_count
          _ -> false
        end
      end,
      timeout
    )
  end

  @doc """
  Wait for batch to be flushed to storage by checking offset advancement.
  This is more reliable than sleeping for batch_timeout.
  """
  def wait_for_batch_flush(topic, partition, initial_offset, timeout \\ 2000) do
    wait_until_with_backoff(
      fn ->
        case SingleFile.get_offsets(topic, partition) do
          {:ok, offsets} -> offsets.latest > initial_offset
          _ -> false
        end
      end,
      timeout,
      10,  # Start with 10ms checks
      100  # Max 100ms between checks
    )
  end

  @doc """
  Get current offset and then wait for new messages to be written.
  Returns the new offset after messages are written.
  """
  def wait_for_new_messages(topic, partition, timeout \\ 2000) do
    {:ok, initial} = SingleFile.get_offsets(topic, partition)

    case wait_for_batch_flush(topic, partition, initial.latest, timeout) do
      :ok ->
        {:ok, final} = SingleFile.get_offsets(topic, partition)
        {:ok, final.latest}
      error ->
        error
    end
  end

  @doc """
  Force flush any pending batches by producing a full batch worth of messages.
  This ensures previous messages in partial batches get written.
  """
  def force_batch_flush(topic, partition) do
    batch_size = Application.get_env(:kafkaesque_core, :max_batch_size, 500)
    flush_messages = Enum.map(1..batch_size, fn i ->
      %{key: "_flush_#{i}", value: "_flush", headers: []}
    end)

    Producer.produce(topic, partition, flush_messages)
    wait_for_messages(topic, partition, batch_size, 5000)
  end

  @doc """
  Consume messages from a topic partition using LogReader.
  """
  def consume_messages(topic, partition, offset, max_messages \\ 100) do
    max_bytes = max_messages * 1000  # Rough estimate
    LogReader.consume(topic, partition, "", offset, max_bytes, 1000)
  end

  @doc """
  Consume messages with a consumer group.
  """
  def consume_messages_with_group(topic, partition, group, offset, max_messages \\ 100) do
    # If offset is -1, use committed offset if available
    starting_offset = if offset == -1 do
      case DetsOffset.fetch(topic, partition, group) do
        {:ok, committed} -> committed + 1
        _ -> 0
      end
    else
      offset
    end

    # Consume messages
    result = consume_messages(topic, partition, starting_offset, max_messages)

    # Auto-commit the last offset
    case result do
      {:ok, messages} when messages != [] ->
        last_offset = starting_offset + length(messages) - 1
        DetsOffset.commit(topic, partition, group, last_offset)
      _ -> :ok
    end

    result
  end

  @doc """
  Start a gRPC connection for testing.
  Only available when testing the server app.
  """
  def connect_grpc do
    # For integration tests, assume the gRPC server is already running
    # via the application supervisor
    Process.sleep(100)

    # Return a mock channel that the tests can use
    # The actual gRPC tests will need to use the GRPC.Stub module directly
    {:ok, :test_channel}
  end

  @doc """
  Make an HTTP request to the REST API using Req or HTTPoison if available,
  falling back to a simple TCP-based implementation.
  """
  def http_request(method, path, body \\ nil, headers \\ []) do
    url = "http://localhost:#{@http_port}#{path}"

    # Use a simpler HTTP client approach
    case make_http_request(method, url, body, headers) do
      {:ok, status, response_body} ->
        {:ok, status, response_body}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp make_http_request(method, url, body, headers) do
    # Parse URL
    uri = URI.parse(url)

    # Build HTTP request
    body_str = if body, do: Jason.encode!(body), else: ""
    method_str = method |> to_string() |> String.upcase()

    request_lines = [
      "#{method_str} #{uri.path || "/"} HTTP/1.1",
      "Host: #{uri.host}:#{uri.port}",
      "Content-Type: application/json",
      "Content-Length: #{byte_size(body_str)}",
      "Connection: close"
    ]

    # Add custom headers
    request_lines = request_lines ++ Enum.map(headers, fn {k, v} -> "#{k}: #{v}" end)

    request = Enum.join(request_lines, "\r\n") <> "\r\n\r\n" <> body_str

    # Make TCP connection
    case :gen_tcp.connect(String.to_charlist(uri.host || "localhost"), uri.port || 80,
                          [:binary, active: false, packet: :raw]) do
      {:ok, socket} ->
        :ok = :gen_tcp.send(socket, request)

        # Read response
        case read_http_response(socket) do
          {:ok, status, body} ->
            :gen_tcp.close(socket)

            # Parse JSON response if applicable
            parsed_body = if body == "" do
              %{}
            else
              case Jason.decode(body) do
                {:ok, decoded} -> decoded
                {:error, _} -> body
              end
            end

            {:ok, status, parsed_body}

          error ->
            :gen_tcp.close(socket)
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_http_response(socket) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, data} ->
        # Parse HTTP response
        [headers_part | body_parts] = String.split(data, "\r\n\r\n", parts: 2)
        [status_line | _headers] = String.split(headers_part, "\r\n")

        # Extract status code
        [_, status_str | _] = String.split(status_line, " ", parts: 3)
        status = String.to_integer(status_str)

        body = if body_parts == [], do: "", else: List.first(body_parts)

        {:ok, status, body}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Assert that a topic exists and has the expected number of partitions.
  """
  def assert_topic_exists(topic_name, expected_partitions) do
    topics = TopicSupervisor.list_topics()
    topic = Enum.find(topics, fn
      {name, _partitions} when is_binary(name) -> name == topic_name
      %{name: name} -> name == topic_name
      _ -> false
    end)

    assert topic != nil, "Topic #{topic_name} does not exist"

    partitions = case topic do
      {_, p} when is_integer(p) -> p
      %{partitions: p} when is_integer(p) -> p
      %{partition_ids: ids} when is_list(ids) -> length(ids)
      _ -> 0
    end

    assert partitions == expected_partitions,
           "Expected #{expected_partitions} partitions, got #{partitions}"
  end

  @doc """
  Get the latest offset for a topic/partition.
  """
  def get_latest_offset(topic, partition) do
    case SingleFile.get_offsets(topic, partition) do
      {:ok, %{latest: latest}} -> latest
      _ -> -1
    end
  end

end
