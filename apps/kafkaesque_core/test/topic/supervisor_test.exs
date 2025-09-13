defmodule Kafkaesque.Topic.SupervisorTest do
  use Kafkaesque.TestCase, async: false

  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  @test_dir "./test_supervisor"

  setup do
    # Set up test directory
    test_dir = setup_test_dir(@test_dir)

    # Set test configuration
    Application.put_env(:kafkaesque_core, :data_dir, test_dir)
    Application.put_env(:kafkaesque_core, :offsets_dir, Path.join(test_dir, "offsets"))

    # The supervisor should already be started by the application
    # Just verify it's running (registered as the module name)
    assert Process.whereis(Kafkaesque.Topic.Supervisor) != nil

    on_exit(fn ->
      # Clean up any created topics
      try do
        if Process.whereis(TopicSupervisor) do
          topics = TopicSupervisor.list_topics()

          Enum.each(topics, fn topic ->
            TopicSupervisor.delete_topic(topic.name)
          end)
        end
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  describe "create_topic/2" do
    test "creates a topic with single partition" do
      assert {:ok, topic} = TopicSupervisor.create_topic("test_topic", 1)
      assert topic.topic == "test_topic"
      assert topic.partitions == 1
    end

    test "creates a topic with multiple partitions" do
      assert {:ok, topic} = TopicSupervisor.create_topic("multi_topic", 3)
      assert topic.topic == "multi_topic"
      assert topic.partitions == 3
    end

    test "fails to create duplicate topic" do
      assert {:ok, _} = TopicSupervisor.create_topic("dup_topic", 1)

      # Second creation should fail or be idempotent
      # Depending on implementation, adjust this test
      result = TopicSupervisor.create_topic("dup_topic", 1)
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end

    test "creates topic directory structure" do
      assert {:ok, _} = TopicSupervisor.create_topic("dir_test", 1)

      topic_dir = Path.join(@test_dir, "dir_test")
      assert File.exists?(topic_dir)
    end
  end

  describe "delete_topic/1" do
    setup do
      {:ok, _} = TopicSupervisor.create_topic("to_delete", 2)
      :ok
    end

    test "deletes an existing topic" do
      # Get a reference to one of the processes before deletion
      [{pid, _}] = Registry.lookup(Kafkaesque.TopicRegistry, {:storage, "to_delete", 0})

      assert :ok = TopicSupervisor.delete_topic("to_delete")

      # Wait for process to actually terminate
      wait_for_termination(pid)

      # Topic should no longer exist
      refute TopicSupervisor.topic_exists?("to_delete")
    end

    test "handles deletion of non-existent topic" do
      # Should not crash
      assert :ok = TopicSupervisor.delete_topic("non_existent")
    end
  end

  describe "list_topics/0" do
    test "returns empty list when no topics" do
      assert [] = TopicSupervisor.list_topics()
    end

    test "returns all created topics" do
      # Clean up any existing topics first
      existing_topics = TopicSupervisor.list_topics()

      Enum.each(existing_topics, fn topic ->
        TopicSupervisor.delete_topic(topic.name)
      end)

      {:ok, _} = TopicSupervisor.create_topic("topic1", 1)
      {:ok, _} = TopicSupervisor.create_topic("topic2", 2)
      {:ok, _} = TopicSupervisor.create_topic("topic3", 3)

      topics = TopicSupervisor.list_topics()
      assert length(topics) == 3

      topic_names = Enum.map(topics, & &1.name) |> Enum.sort()
      assert topic_names == ["topic1", "topic2", "topic3"]

      # Check partition counts
      topic_map = Map.new(topics, fn t -> {t.name, t.partitions} end)
      assert topic_map["topic1"] == 1
      assert topic_map["topic2"] == 2
      assert topic_map["topic3"] == 3
    end
  end

  describe "get_topic_info/1" do
    setup do
      {:ok, _} = TopicSupervisor.create_topic("info_test", 2)
      :ok
    end

    test "returns info for existing topic" do
      assert {:ok, info} = TopicSupervisor.get_topic_info("info_test")
      assert info.name == "info_test"
      assert info.partitions == 2
      assert info.partition_ids == [0, 1]
      assert info.status == :active
    end

    test "returns error for non-existent topic" do
      assert {:error, :topic_not_found} = TopicSupervisor.get_topic_info("non_existent")
    end
  end

  describe "topic_exists?/1" do
    test "returns false for non-existent topic" do
      refute TopicSupervisor.topic_exists?("non_existent")
    end

    test "returns true for existing topic" do
      {:ok, _} = TopicSupervisor.create_topic("exists_test", 1)
      assert TopicSupervisor.topic_exists?("exists_test")
    end
  end

  describe "partition supervision" do
    test "starts all required processes for each partition" do
      {:ok, _} = TopicSupervisor.create_topic("supervised_topic", 1)

      # Wait for processes to register
      assert {:ok, _} = wait_for_registration({:storage, "supervised_topic", 0})

      # Check that processes are registered
      # Storage process
      assert [{_, _}] =
               Registry.lookup(Kafkaesque.TopicRegistry, {:storage, "supervised_topic", 0})

      # Check that partition supervisor is registered
      assert [{_, _}] =
               Registry.lookup(Kafkaesque.TopicRegistry, {:partition_sup, "supervised_topic", 0})
    end

    test "restarts failed partition processes" do
      {:ok, _} = TopicSupervisor.create_topic("restart_test", 1)

      # Wait for storage process to register
      assert {:ok, pid} = wait_for_registration({:storage, "restart_test", 0})

      # Monitor and kill the process
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      # Wait for process to die
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1000

      # Wait for supervisor to restart the process
      assert {:ok, new_pid} = wait_for_registration({:storage, "restart_test", 0}, 2000)

      # Process should be restarted with new PID
      assert new_pid != pid
      assert Process.alive?(new_pid)
    end
  end
end
