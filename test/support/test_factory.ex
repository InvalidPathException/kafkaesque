defmodule Kafkaesque.Test.Factory do
  @moduledoc """
  Factory for generating test data.
  """

  @doc """
  Generate a unique topic name.
  """
  def unique_topic_name(prefix \\ "test-topic") do
    "#{prefix}-#{System.unique_integer([:positive])}-#{:rand.uniform(9999)}"
  end

  @doc """
  Generate a unique consumer group name.
  """
  def unique_group_name(prefix \\ "test-group") do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  @doc """
  Generate test messages with consistent structure.
  """
  def generate_messages(count, opts \\ []) do
    key_prefix = Keyword.get(opts, :key_prefix, "key")
    value_prefix = Keyword.get(opts, :value_prefix, "value")
    with_headers = Keyword.get(opts, :with_headers, true)

    Enum.map(1..count, fn i ->
      %{
        key: "#{key_prefix}-#{i}",
        value: "#{value_prefix}-#{i}",
        headers: if(with_headers, do: [{"header-#{i}", "hvalue-#{i}"}], else: []),
        timestamp_ms: System.system_time(:millisecond)
      }
    end)
  end

  @doc """
  Generate a test record for gRPC/REST.
  """
  def generate_record(opts \\ []) do
    %{
      key: Keyword.get(opts, :key, "test-key-#{:rand.uniform(9999)}"),
      value: Keyword.get(opts, :value, "test-value-#{:rand.uniform(9999)}"),
      headers: Keyword.get(opts, :headers, []),
      timestamp_ms: Keyword.get(opts, :timestamp_ms, System.system_time(:millisecond))
    }
  end

  @doc """
  Generate batch of records for produce requests.
  """
  def generate_batch(size, opts \\ []) do
    Enum.map(1..size, fn i ->
      generate_record(Keyword.put(opts, :key, "batch-key-#{i}"))
    end)
  end

  @doc """
  Create a test topic with guaranteed cleanup.
  """
  def create_test_topic(opts \\ []) do
    name = Keyword.get(opts, :name, unique_topic_name())
    partitions = Keyword.get(opts, :partitions, 1)

    case Kafkaesque.Topic.Supervisor.create_topic(name, partitions) do
      {:ok, info} ->
        {:ok, info}

      {:error, :already_exists} ->
        # Clean up and retry
        Kafkaesque.Topic.Supervisor.delete_topic(name)
        Process.sleep(100)
        Kafkaesque.Topic.Supervisor.create_topic(name, partitions)

      error ->
        error
    end
  end
end
