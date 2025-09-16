defmodule KafkaesqueServer.TopicControllerTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias Kafkaesque.Test.Helpers
  alias KafkaesqueServer.TopicController

  setup do
    context = Helpers.setup_test()

    # Clean up any existing topics from previous runs
    topics = Kafkaesque.Topic.Supervisor.list_topics()
    for topic <- topics do
      name = case topic do
        {n, _} -> n
        %{name: n} -> n
        _ -> nil
      end
      if name, do: Helpers.delete_test_topic(name)
    end

    on_exit(fn ->
      Helpers.cleanup_test(context)
    end)

    {:ok, context: context}
  end

  describe "create/2" do
    test "creates topic with valid parameters" do
      topic_name = "test-topic-#{:rand.uniform(999_999)}"
      conn = conn(:post, "/v1/topics", %{
        "name" => topic_name,
        "partitions" => 3
      })
      |> put_req_header("content-type", "application/json")

      conn = TopicController.create(conn, %{
        "name" => topic_name,
        "partitions" => 3
      })

      assert conn.status == 201
      response = Jason.decode!(conn.resp_body)
      assert response["topic"] == topic_name
      assert response["partitions"] == 3
      assert response["status"] == "created"

      # Verify topic was created
      Helpers.assert_topic_exists(topic_name, 3)
    end

    test "returns error when topic name is missing" do
      conn = conn(:post, "/v1/topics", %{
        "partitions" => 1
      })
      |> put_req_header("content-type", "application/json")

      conn = TopicController.create(conn, %{
        "partitions" => 1
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["error"] == "Topic name is required"
    end

    test "returns error when topic name is empty" do
      conn = conn(:post, "/v1/topics", %{
        "name" => "",
        "partitions" => 1
      })
      |> put_req_header("content-type", "application/json")

      conn = TopicController.create(conn, %{
        "name" => "",
        "partitions" => 1
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["error"] == "Topic name is required"
    end

    test "defaults to 1 partition when not specified" do
      topic_name = "default-partitions-#{:rand.uniform(999_999)}"
      conn = conn(:post, "/v1/topics", %{
        "name" => topic_name
      })
      |> put_req_header("content-type", "application/json")

      conn = TopicController.create(conn, %{
        "name" => topic_name
      })

      assert conn.status == 201
      response = Jason.decode!(conn.resp_body)
      assert response["partitions"] == 1
    end

    test "handles topic creation failure" do
      topic_name = "duplicate-#{:rand.uniform(999_999)}"
      # Create topic first
      {:ok, _, _} = Helpers.create_test_topic(name: topic_name, partitions: 1)

      conn = conn(:post, "/v1/topics", %{
        "name" => topic_name,
        "partitions" => 1
      })
      |> put_req_header("content-type", "application/json")

      conn = TopicController.create(conn, %{
        "name" => topic_name,
        "partitions" => 1
      })

      assert conn.status == 500
      response = Jason.decode!(conn.resp_body)
      assert response["error"] =~ "Failed to create topic"
    end
  end

  describe "index/2" do
    test "returns empty list when no topics exist" do
      # Clean up all existing topics first
      topics = Kafkaesque.Topic.Supervisor.list_topics()
      for topic <- topics do
        name = case topic do
          {n, _} -> n
          %{name: n} -> n
          _ -> nil
        end
        if name, do: Helpers.delete_test_topic(name)
      end

      # Give time for cleanup
      Process.sleep(100)

      conn = conn(:get, "/v1/topics")
      conn = TopicController.index(conn, %{})

      assert conn.status == 200
      response = Jason.decode!(conn.resp_body)
      assert response["topics"] == []
    end

    test "returns all topics" do
      suffix = :rand.uniform(999_999)
      # Create test topics
      {:ok, _topic1, _} = Helpers.create_test_topic(name: "topic1-#{suffix}", partitions: 1)
      {:ok, _topic2, _} = Helpers.create_test_topic(name: "topic2-#{suffix}", partitions: 2)
      {:ok, _topic3, _} = Helpers.create_test_topic(name: "topic3-#{suffix}", partitions: 3)

      conn = conn(:get, "/v1/topics")
      conn = TopicController.index(conn, %{})

      assert conn.status == 200
      response = Jason.decode!(conn.resp_body)
      assert length(response["topics"]) >= 3

      # Verify topic details
      topic_map = Map.new(response["topics"], fn t ->
        {t["name"], t["partitions"]}
      end)

      assert topic_map["topic1-#{suffix}"] == 1
      assert topic_map["topic2-#{suffix}"] == 2
      assert topic_map["topic3-#{suffix}"] == 3
    end

    test "handles various topic formats correctly" do
      topic_name = "format-test-#{:rand.uniform(999_999)}"
      # Create topics to test format handling
      {:ok, _, _} = Helpers.create_test_topic(name: topic_name, partitions: 2)

      conn = conn(:get, "/v1/topics")
      conn = TopicController.index(conn, %{})

      response = Jason.decode!(conn.resp_body)
      topic = Enum.find(response["topics"], fn t ->
        t["name"] == topic_name
      end)

      assert topic != nil
      assert topic["name"] == topic_name
      assert topic["partitions"] == 2
    end
  end
end
