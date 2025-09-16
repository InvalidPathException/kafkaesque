defmodule DebugConsumeTest do
  use ExUnit.Case, async: false

  alias Kafkaesque.Pipeline.Producer
  alias Kafkaesque.Storage.SingleFile
  alias Kafkaesque.Test.{Factory, Helpers}
  alias Kafkaesque.Topic.LogReader

  require Logger

  setup do
    context = Helpers.setup_test()
    {:ok, topic, _info} = Helpers.create_test_topic()

    on_exit(fn ->
      Helpers.delete_test_topic(topic)
      Helpers.cleanup_test(context)
    end)

    {:ok, topic: topic, context: context}
  end

  test "debug message production and consumption", %{topic: topic} do
    partition = 0

    # Generate 25 messages
    messages = Factory.generate_messages(25)
    Logger.debug("Producing #{length(messages)} messages")

    # Check initial state
    {:ok, initial_offsets} = SingleFile.get_offsets(topic, partition)
    Logger.debug("Initial offsets: earliest=#{initial_offsets.earliest}, latest=#{initial_offsets.latest}")

    # Produce messages
    {:ok, result} = Producer.produce(topic, partition, messages)
    Logger.debug("Produce result: #{inspect(result)}")

    # Wait for flush
    Process.sleep(500)

    # Check offsets after production
    {:ok, after_produce} = SingleFile.get_offsets(topic, partition)
    Logger.debug("After produce offsets: earliest=#{after_produce.earliest}, latest=#{after_produce.latest}")

    # Try to read directly from storage
    Logger.debug("Reading from storage")
    {:ok, storage_records} = SingleFile.read(topic, partition, 0, 1_048_576)
    Logger.debug("Storage returned #{length(storage_records)} records")

    # Try to read via LogReader
    Logger.debug("Reading via LogReader")
    {:ok, reader_records} = LogReader.consume(topic, partition, "", 0, 1_048_576, 100)
    Logger.debug("LogReader returned #{length(reader_records)} records")

    # Check the actual records
    if length(reader_records) < 25 do
      Logger.warning("Missing messages! Expected 25, got #{length(reader_records)}")

      # Check what offsets we got
      offsets = Enum.map(reader_records, & &1[:offset])
      Logger.debug("Received offsets: #{inspect(offsets)}")

      # Check file size
      data_dir = Application.get_env(:kafkaesque_core, :data_dir)
      file_path = Path.join([data_dir, topic, "#{partition}.log"])
      %{size: file_size} = File.stat!(file_path)
      Logger.debug("Log file size: #{file_size} bytes")
    end

    assert length(reader_records) == 25, "Should get all 25 messages"
  end
end
