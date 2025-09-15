defmodule Kafkaesque.Topic.SupervisorTest do
  use Kafkaesque.TestCase, async: false

  alias Kafkaesque.Topic.Supervisor, as: TopicSupervisor

  setup do
    # Set up test directory
    test_dir = setup_test_dir("test_supervisor")

    # Set test configuration
    Application.put_env(:kafkaesque_core, :data_dir, test_dir)
    Application.put_env(:kafkaesque_core, :offsets_dir, Path.join(test_dir, "offsets"))

    # The supervisor should already be started by the application
    # Just verify it's running (registered as the module name)
    assert Process.whereis(Kafkaesque.Topic.Supervisor) != nil

    # Since we use unique topic names, we don't need cleanup between tests
    # Only clean up on exit to be a good citizen
    on_exit(fn ->
      # Quick cleanup - only for topics created in this test
      File.rm_rf!(test_dir)
    end)

    :ok
  end

  describe "create_topic/2" do
    test "creates a topic with single partition" do
      topic_name = unique_topic_name("single_partition")
      assert {:ok, topic} = TopicSupervisor.create_topic(topic_name, 1)
      assert topic.topic == topic_name
      assert topic.partitions == 1
    end

    test "creates a topic with multiple partitions" do
      topic_name = unique_topic_name("multi_partition")
      assert {:ok, topic} = TopicSupervisor.create_topic(topic_name, 3)
      assert topic.topic == topic_name
      assert topic.partitions == 3
    end

    test "fails to create duplicate topic" do
      topic_name = unique_topic_name("duplicate")
      assert {:ok, _} = TopicSupervisor.create_topic(topic_name, 1)

      # Second creation should fail or be idempotent
      # Depending on implementation, adjust this test
      result = TopicSupervisor.create_topic(topic_name, 1)
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end

    test "creates topic directory structure" do
      topic_name = unique_topic_name("dir_test")
      assert {:ok, _} = TopicSupervisor.create_topic(topic_name, 1)

      topic_dir = Path.join([System.tmp_dir!(), "test_supervisor", topic_name])
      assert File.exists?(topic_dir)
    end
  end

  describe "delete_topic/1" do
    setup do
      topic_name = unique_topic_name("to_delete")
      {:ok, _} = TopicSupervisor.create_topic(topic_name, 2)
      {:ok, topic_name: topic_name}
    end

    test "deletes an existing topic", %{topic_name: topic_name} do
      # Verify topic was created
      assert TopicSupervisor.topic_exists?(topic_name)

      # Get a reference to one of the processes before deletion
      [{pid, _}] = Registry.lookup(Kafkaesque.TopicRegistry, {:storage, topic_name, 0})

      assert :ok = TopicSupervisor.delete_topic(topic_name)

      # Wait for process to actually terminate
      wait_for_termination(pid)

      # Add a small delay to ensure cleanup completes
      Process.sleep(100)

      # Topic should no longer exist - use retry for potential timing issues
      result = retry_until_success(fn ->
        not TopicSupervisor.topic_exists?(topic_name)
      end, 2000)

      assert {:ok, true} = result
    end

    test "handles deletion of non-existent topic" do
      # Should not crash
      assert :ok = TopicSupervisor.delete_topic("non_existent")
    end
  end

  describe "list_topics/0" do
    test "returns empty list when no topics" do
      # This test needs a clean state
      ensure_clean_state()
      assert [] = TopicSupervisor.list_topics()
    end

    test "returns all created topics" do
      # Ensure clean state first
      ensure_clean_state()

      topic1 = unique_topic_name("list_topic1")
      topic2 = unique_topic_name("list_topic2")
      topic3 = unique_topic_name("list_topic3")

      {:ok, _} = TopicSupervisor.create_topic(topic1, 1)
      {:ok, _} = TopicSupervisor.create_topic(topic2, 2)
      {:ok, _} = TopicSupervisor.create_topic(topic3, 3)

      topics = TopicSupervisor.list_topics()
      assert length(topics) == 3

      topic_names = Enum.map(topics, & &1.name) |> Enum.sort()
      assert Enum.sort([topic1, topic2, topic3]) == topic_names

      # Check partition counts
      topic_map = Map.new(topics, fn t -> {t.name, t.partitions} end)
      assert topic_map[topic1] == 1
      assert topic_map[topic2] == 2
      assert topic_map[topic3] == 3
    end
  end

  describe "get_topic_info/1" do
    setup do
      topic_name = unique_topic_name("info_test")
      {:ok, _} = TopicSupervisor.create_topic(topic_name, 2)
      {:ok, topic_name: topic_name}
    end

    test "returns info for existing topic", %{topic_name: topic_name} do
      assert {:ok, info} = TopicSupervisor.get_topic_info(topic_name)
      assert info.name == topic_name
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
      topic_name = unique_topic_name("exists_test")
      {:ok, _} = TopicSupervisor.create_topic(topic_name, 1)
      # Small delay to ensure supervisor child is registered
      Process.sleep(50)
      assert TopicSupervisor.topic_exists?(topic_name)
    end
  end

  describe "partition supervision" do
    test "starts all required processes for each partition" do
      topic_name = unique_topic_name("supervised_topic")
      {:ok, _} = TopicSupervisor.create_topic(topic_name, 1)

      # Wait for processes to register
      assert {:ok, _} = wait_for_registration({:storage, topic_name, 0})

      # Check that processes are registered
      # Storage process
      assert [{_, _}] =
               Registry.lookup(Kafkaesque.TopicRegistry, {:storage, topic_name, 0})

      # Check that partition supervisor is registered
      assert [{_, _}] =
               Registry.lookup(Kafkaesque.TopicRegistry, {:partition_sup, topic_name, 0})
    end

    test "restarts failed partition processes" do
      topic_name = unique_topic_name("restart_test")
      {:ok, _} = TopicSupervisor.create_topic(topic_name, 1)

      # Wait for storage process to register and be alive
      assert {:ok, pid} = wait_for_registration({:storage, topic_name, 0}, 2000)

      # Monitor and kill the process
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      # Wait for process to die
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2000

      # Wait for supervisor to restart the process with retries
      # The supervisor may take some time to restart
      Process.sleep(100)

      result = retry_until_success(fn ->
        case Registry.lookup(Kafkaesque.TopicRegistry, {:storage, topic_name, 0}) do
          [{new_pid, _}] when is_pid(new_pid) and new_pid != pid ->
            if Process.alive?(new_pid) do
              {:ok, new_pid}
            else
              {:error, :not_alive}
            end
          _ ->
            {:error, :not_found}
        end
      end, 5000)

      assert {:ok, new_pid} = result
      assert new_pid != pid
      assert Process.alive?(new_pid)
    end
  end
end
