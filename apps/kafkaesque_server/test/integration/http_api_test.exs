defmodule KafkaesqueServer.Integration.HttpApiTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Kafkaesque.Offsets.DetsOffset
  alias Kafkaesque.Test.{Client, Factory, Helpers}
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

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

    # Create unique topic for this test
    {:ok, topic, _info} = Helpers.create_test_topic()

    on_exit(fn ->
      Helpers.delete_test_topic(topic)
      Helpers.cleanup_test(context)
    end)

    {:ok, topic: topic, context: context}
  end

  describe "HTTP API Consumer Group" do
    test "auto-commit behavior works correctly", %{topic: topic} do
      partition = 0
      group = Factory.unique_group_name("auto-commit")

      # Produce messages and wait for them to be written
      messages = Factory.generate_messages(25)
      {:ok, _} = Helpers.produce_and_wait(topic, partition, messages)

      # Consume with auto-commit via HTTP API
      {:ok, 200, response} = Client.consume_via_http(topic,
        group: group,
        offset: 0,
        auto_commit: true
      )

      assert length(response["records"]) == 25

      # Check that offset was auto-committed
      {:ok, committed_offset} = DetsOffset.fetch(topic, partition, group)
      assert committed_offset == 24  # Last message offset

      # Produce more messages
      more_messages = Factory.generate_messages(10, key_prefix: "new", value_prefix: "new")
      {:ok, _} = Helpers.produce_and_wait(topic, partition, more_messages)

      # Consume again - should start from committed offset + 1
      {:ok, 200, response2} = Client.consume_via_http(topic,
        group: group,
        offset: -1  # Use latest, should use committed offset
      )

      # Should only get the new messages
      assert length(response2["records"]) == 10
      assert List.first(response2["records"])["key"] == "new-1"
    end
  end
end
