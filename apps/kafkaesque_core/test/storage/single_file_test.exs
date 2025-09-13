defmodule Kafkaesque.Storage.SingleFileTest do
  use ExUnit.Case, async: false

  alias Kafkaesque.Storage.SingleFile

  @test_topic "test_topic"
  @test_partition 0

  setup_all do
    # Start Registry once for all tests
    case Registry.start_link(keys: :unique, name: Kafkaesque.TopicRegistry) do
      {:ok, pid} ->
        on_exit(fn -> Process.exit(pid, :normal) end)
        {:ok, registry: pid}

      {:error, {:already_started, pid}} ->
        {:ok, registry: pid}
    end
  end

  setup do
    test_dir = setup_test_dir()
    {:ok, test_dir: test_dir}
  end

  defp setup_test_dir do
    test_dir = "./test_#{:erlang.unique_integer([:positive])}"
    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)
    on_exit(fn -> File.rm_rf!(test_dir) end)
    test_dir
  end

  defp create_test_records(count, prefix \\ "test") do
    Enum.map(1..count, fn i ->
      %{
        key: "#{prefix}_key_#{i}",
        value: "#{prefix}_value_#{i}",
        headers: [{"header_#{i}", "value_#{i}"}],
        timestamp_ms: System.system_time(:millisecond)
      }
    end)
  end

  defp wait_for_termination(pid, timeout \\ 1000) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} ->
        :ok
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        {:error, :timeout}
    end
  end

  describe "start_link/1" do
    test "starts the storage process", %{test_dir: test_dir} do
      assert {:ok, pid} =
               SingleFile.start_link(
                 topic: @test_topic,
                 partition: @test_partition,
                 data_dir: test_dir
               )

      assert Process.alive?(pid)
    end

    test "creates the topic directory", %{test_dir: test_dir} do
      {:ok, _pid} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      assert File.exists?(Path.join(test_dir, @test_topic))
    end
  end

  describe "append/3" do
    setup %{test_dir: test_dir} do
      {:ok, _pid} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      :ok
    end

    test "appends single record" do
      record = %{
        key: "key1",
        value: "value1",
        headers: [{"header1", "value1"}],
        timestamp_ms: 1_234_567_890
      }

      assert {:ok, result} = SingleFile.append(@test_topic, @test_partition, [record])
      assert result.topic == @test_topic
      assert result.partition == @test_partition
      assert result.base_offset == 0
      assert result.count == 1
    end

    test "appends multiple records" do
      records = [
        %{key: "key1", value: "value1"},
        %{key: "key2", value: "value2"},
        %{key: "key3", value: "value3"}
      ]

      assert {:ok, result} = SingleFile.append(@test_topic, @test_partition, records)
      assert result.count == 3
      assert result.base_offset == 0

      # Append more records
      more_records = [
        %{key: "key4", value: "value4"},
        %{key: "key5", value: "value5"}
      ]

      assert {:ok, result2} = SingleFile.append(@test_topic, @test_partition, more_records)
      assert result2.base_offset == 3
      assert result2.count == 2
    end

    test "handles empty values" do
      record = %{key: nil, value: nil}
      assert {:ok, _result} = SingleFile.append(@test_topic, @test_partition, [record])
    end
  end

  describe "read/4" do
    setup %{test_dir: test_dir} do
      {:ok, _pid} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      # Add some test data
      records = create_test_records(5)
      {:ok, _} = SingleFile.append(@test_topic, @test_partition, records)
      :ok
    end

    test "reads from specific offset" do
      assert {:ok, records} = SingleFile.read(@test_topic, @test_partition, 0, 1024)
      assert length(records) > 0
      assert List.first(records).value == "test_value_1"
    end

    test "reads with max_bytes limit" do
      # Read with very small limit
      assert {:ok, records} = SingleFile.read(@test_topic, @test_partition, 0, 100)
      assert length(records) >= 1
    end

    test "returns error for out of range offset" do
      assert {:error, :offset_out_of_range} =
               SingleFile.read(@test_topic, @test_partition, 1000, 1024)
    end
  end

  describe "get_offsets/2" do
    setup %{test_dir: test_dir} do
      {:ok, _pid} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      :ok
    end

    test "returns offsets for empty partition" do
      assert {:ok, offsets} = SingleFile.get_offsets(@test_topic, @test_partition)
      assert offsets.earliest == 0
      assert offsets.latest == 0
    end

    test "returns offsets after appending" do
      records = [
        %{key: "key1", value: "value1"},
        %{key: "key2", value: "value2"}
      ]

      {:ok, _} = SingleFile.append(@test_topic, @test_partition, records)

      assert {:ok, offsets} = SingleFile.get_offsets(@test_topic, @test_partition)
      assert offsets.earliest == 0
      assert offsets.latest == 2
    end
  end

  describe "persistence" do
    test "recovers state after restart" do
      test_dir = setup_test_dir()

      # Start and write data
      {:ok, pid1} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      records = create_test_records(2)
      {:ok, _} = SingleFile.append(@test_topic, @test_partition, records)

      # Stop the process gracefully
      :ok = SingleFile.close(@test_topic, @test_partition)
      wait_for_termination(pid1)

      # Unregister from registry to allow restart with same name
      Registry.unregister(Kafkaesque.TopicRegistry, {:storage, @test_topic, @test_partition})

      # Start again
      {:ok, _pid2} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      # Should be able to read old data
      assert {:ok, read_records} = SingleFile.read(@test_topic, @test_partition, 0, 1024)
      assert length(read_records) == 2

      # Should continue from correct offset
      new_record = %{key: "key3", value: "value3"}
      assert {:ok, result} = SingleFile.append(@test_topic, @test_partition, [new_record])
      assert result.base_offset == 2
    end
  end

  describe "frame serialization" do
    test "correctly serializes and deserializes records with all fields" do
      test_dir = setup_test_dir()

      {:ok, _pid} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      record = %{
        key: "test_key",
        value: "test_value",
        headers: [
          {"header1", "value1"},
          {"header2", "value2"}
        ],
        timestamp_ms: 1_234_567_890
      }

      {:ok, _} = SingleFile.append(@test_topic, @test_partition, [record])
      {:ok, [read_record]} = SingleFile.read(@test_topic, @test_partition, 0, 1024)

      assert read_record.key == "test_key"
      assert read_record.value == "test_value"
      assert length(read_record.headers) == 2
      assert read_record.timestamp_ms == 1_234_567_890
    end

    test "handles binary data correctly" do
      test_dir = setup_test_dir()

      {:ok, _pid} =
        SingleFile.start_link(
          topic: @test_topic,
          partition: @test_partition,
          data_dir: test_dir
        )

      binary_data = <<0, 1, 2, 3, 255, 254, 253>>

      record = %{
        key: binary_data,
        value: binary_data
      }

      {:ok, _} = SingleFile.append(@test_topic, @test_partition, [record])
      {:ok, [read_record]} = SingleFile.read(@test_topic, @test_partition, 0, 1024)

      assert read_record.key == binary_data
      assert read_record.value == binary_data
    end
  end
end
