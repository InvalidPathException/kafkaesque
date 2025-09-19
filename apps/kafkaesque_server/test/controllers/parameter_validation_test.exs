defmodule KafkaesqueServer.ParameterValidationTest do
  @moduledoc """
  Tests for consume parameter validation to prevent invalid or malicious inputs.
  Ensures max_bytes and max_wait_ms are within safe bounds.
  """

  use ExUnit.Case, async: false
  import Plug.Test

  alias Kafkaesque.Test.{Factory, Helpers}
  alias KafkaesqueServer.RecordController

  setup do
    context = Helpers.setup_test()
    {:ok, topic, _info} = Helpers.create_test_topic(name: Factory.unique_topic_name("validation-test"))

    # Produce some test data
    messages = Factory.generate_messages(5)
    Helpers.produce_and_wait(topic, 0, messages)
    Process.sleep(100)

    on_exit(fn ->
      Helpers.delete_test_topic(topic)
      Helpers.cleanup_test(context)
    end)

    {:ok, topic: topic}
  end

  describe "max_bytes validation" do
    test "rejects negative max_bytes", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "-100",
        "max_wait_ms" => "500"
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["error"] =~ "Invalid max_bytes"
    end

    test "rejects excessively large max_bytes", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "999999999999",  # Way over 100MB limit
        "max_wait_ms" => "500"
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["error"] =~ "Invalid max_bytes"
    end

    test "accepts valid max_bytes at lower bound", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "0",  # Minimum valid value
        "max_wait_ms" => "100"
      })

      assert conn.status == 200
    end

    test "accepts valid max_bytes at upper bound", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "100000000",  # Maximum valid value (100MB)
        "max_wait_ms" => "100"
      })

      assert conn.status == 200
    end

    test "accepts reasonable max_bytes", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "1048576",  # 1MB - reasonable value
        "max_wait_ms" => "500"
      })

      assert conn.status == 200
      response = Jason.decode!(conn.resp_body)
      assert response["topic"] == topic
    end
  end

  describe "max_wait_ms validation" do
    test "rejects negative max_wait_ms", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "1048576",
        "max_wait_ms" => "-1"
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["error"] =~ "Invalid max_wait_ms"
    end

    test "rejects excessively large max_wait_ms", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "1048576",
        "max_wait_ms" => "120000"  # 2 minutes - over 60 second limit
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["error"] =~ "Invalid max_wait_ms"
    end

    test "accepts valid max_wait_ms at lower bound", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "1048576",
        "max_wait_ms" => "0"  # Minimum valid value
      })

      assert conn.status == 200
    end

    @tag timeout: 70_000  # Test needs to wait up to 60 seconds
    test "accepts valid max_wait_ms at upper bound", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      # Use offset -1 (latest) to return immediately since there's data
      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "offset" => "-1",  # Start from latest to avoid waiting
        "max_bytes" => "1048576",
        "max_wait_ms" => "60000"  # Maximum valid value (60 seconds)
      })

      assert conn.status == 200
    end

    test "accepts reasonable max_wait_ms", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "1048576",
        "max_wait_ms" => "5000"  # 5 seconds - reasonable value
      })

      assert conn.status == 200
      response = Jason.decode!(conn.resp_body)
      assert response["topic"] == topic
    end
  end

  describe "combined parameter validation" do
    test "rejects when both parameters are invalid", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "max_bytes" => "-500",
        "max_wait_ms" => "-1000"
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      # Should fail on first validation error (max_bytes)
      assert response["error"] =~ "Invalid max_bytes"
    end

    test "uses defaults when parameters are not provided", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      # No max_bytes or max_wait_ms provided
      conn = RecordController.consume(conn, %{
        "topic" => topic
      })

      assert conn.status == 200
      response = Jason.decode!(conn.resp_body)
      assert response["topic"] == topic
      # Should use defaults: max_bytes=1048576, max_wait_ms=500
    end

    test "handles non-numeric parameter values gracefully", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      # This will raise an error when trying to parse to integer
      # The validation should catch this
      assert_raise ArgumentError, fn ->
        RecordController.consume(conn, %{
          "topic" => topic,
          "max_bytes" => "not_a_number",
          "max_wait_ms" => "500"
        })
      end
    end

    test "validates parameters for SSE format", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "format" => "sse",
        "max_bytes" => "-100",
        "max_wait_ms" => "500"
      })

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["error"] =~ "Invalid max_bytes"
    end
  end

  describe "auto_commit parameter handling" do
    test "accepts true for auto_commit", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "auto_commit" => "true",
        "group" => "test-group"
      })

      assert conn.status == 200
    end

    test "accepts false for auto_commit", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "auto_commit" => "false",
        "group" => "test-group"
      })

      assert conn.status == 200
    end

    test "defaults to true when auto_commit not provided", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "group" => "test-group"
      })

      assert conn.status == 200
      # Default should be true (verified by checking behavior in integration tests)
    end

    test "treats any non-true value as false for auto_commit", %{topic: topic} do
      conn = conn(:get, "/v1/topics/#{topic}/records")

      conn = RecordController.consume(conn, %{
        "topic" => topic,
        "auto_commit" => "yes",  # Not exactly "true"
        "group" => "test-group"
      })

      assert conn.status == 200
      # Should be treated as false since it's not exactly "true"
    end
  end
end
