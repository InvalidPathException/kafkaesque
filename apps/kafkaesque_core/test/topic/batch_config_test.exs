defmodule Kafkaesque.Topic.BatchConfigTest do
  use ExUnit.Case, async: false
  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  setup do
    on_exit(fn ->
      # Clean up any test topics
      TopicSupervisor.list_topics()
      |> Enum.each(fn
        {topic, _} when is_binary(topic) ->
          if String.starts_with?(topic, "batch_config_test") do
            TopicSupervisor.delete_topic(topic)
          end
        %{name: topic} when is_binary(topic) ->
          if String.starts_with?(topic, "batch_config_test") do
            TopicSupervisor.delete_topic(topic)
          end
        _ ->
          :ok
      end)
    end)

    :ok
  end

  describe "per-topic batch configuration" do
    test "create_topic accepts batch configuration options" do
      topic = "batch_config_test_#{System.unique_integer([:positive])}"

      # Create topic with custom batch settings
      opts = [
        batch_size: 100,
        batch_timeout: 1000,
        min_demand: 10,
        max_demand: 200
      ]

      assert {:ok, _} = TopicSupervisor.create_topic(topic, 1, opts)

      # Verify the topic was created
      topics = TopicSupervisor.list_topics()
      assert Enum.any?(topics, fn
        {name, _} -> name == topic
        %{name: name} -> name == topic
      end)

      # Get the BatchingConsumer process via Registry
      [{consumer_pid, _}] = Registry.lookup(Kafkaesque.TopicRegistry, {:batching_consumer, topic, 0})
      assert consumer_pid != nil

      # Verify the BatchingConsumer state has the correct config
      gen_stage_state = :sys.get_state(consumer_pid)
      consumer_state = gen_stage_state.state
      assert consumer_state.batch_size == 100
      assert consumer_state.batch_timeout == 1000
    end

    test "create_topic uses defaults when no options provided" do
      topic = "batch_config_test_default_#{System.unique_integer([:positive])}"

      assert {:ok, _} = TopicSupervisor.create_topic(topic, 1)

      # Get the BatchingConsumer process via Registry
      [{consumer_pid, _}] = Registry.lookup(Kafkaesque.TopicRegistry, {:batching_consumer, topic, 0})
      assert consumer_pid != nil

      # Verify defaults are used
      gen_stage_state = :sys.get_state(consumer_pid)
      consumer_state = gen_stage_state.state
      default_batch_size = Application.get_env(:kafkaesque_core, :max_batch_size, 500)
      default_timeout = Application.get_env(:kafkaesque_core, :batch_timeout, 5000)

      assert consumer_state.batch_size == default_batch_size
      assert consumer_state.batch_timeout == default_timeout
    end
  end
end
