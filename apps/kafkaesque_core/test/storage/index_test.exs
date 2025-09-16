defmodule Kafkaesque.Storage.IndexTest do
  use ExUnit.Case, async: false

  alias Kafkaesque.Storage.Index

  @test_topic "test_topic"
  @test_partition 0

  describe "new/2" do
    test "creates a new index" do
      index = Index.new(@test_topic, @test_partition)

      assert index.topic == @test_topic
      assert index.partition == @test_partition
      assert index.min_offset == nil
      assert index.max_offset == nil
    end
  end

  describe "add/4" do
    test "adds entries to the index" do
      index = Index.new(@test_topic, @test_partition)

      index = Index.add(index, 0, 0, 100)
      assert index.min_offset == 0
      assert index.max_offset == 0

      index = Index.add(index, 10, 1000, 150)
      assert index.min_offset == 0
      assert index.max_offset == 10

      index = Index.add(index, 5, 500, 120)
      assert index.min_offset == 0
      assert index.max_offset == 10
    end
  end

  describe "lookup/2" do
    setup do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(0, 0, 100)
        |> Index.add(10, 1000, 150)
        |> Index.add(20, 2000, 200)
        |> Index.add(30, 3000, 180)

      {:ok, index: index}
    end

    test "finds exact match", %{index: index} do
      assert {:ok, {0, 0}} = Index.lookup(index, 0)
      assert {:ok, {1000, 10}} = Index.lookup(index, 10)
      assert {:ok, {2000, 20}} = Index.lookup(index, 20)
      assert {:ok, {3000, 30}} = Index.lookup(index, 30)
    end

    test "finds closest entry before offset", %{index: index} do
      # Offset 15 should find entry at offset 10
      assert {:ok, {position, offset}} = Index.lookup(index, 15)
      # Should be at position 1000 with offset 10
      assert position == 1000
      assert offset == 10

      # Offset 25 should find entry at offset 20
      assert {:ok, {position, offset}} = Index.lookup(index, 25)
      assert position == 2000
      assert offset == 20
    end

    test "returns not_found for offset before min", %{index: index} do
      assert :not_found = Index.lookup(index, -1)
    end

    test "returns closest entry for offset after max", %{index: index} do
      # Index returns the closest entry before the requested offset
      # SingleFile handles checking if offset is actually out of range
      assert {:ok, {3000, _length}} = Index.lookup(index, 100)
    end

    test "handles empty index" do
      empty_index = Index.new(@test_topic, @test_partition)
      assert :not_found = Index.lookup(empty_index, 0)
    end
  end

  describe "get_range/1" do
    test "returns range for populated index" do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(5, 500, 100)
        |> Index.add(15, 1500, 150)
        |> Index.add(10, 1000, 120)

      assert {5, 15} = Index.get_range(index)
    end

    test "returns {0, 0} for empty index" do
      index = Index.new(@test_topic, @test_partition)
      assert {0, 0} = Index.get_range(index)
    end
  end

  describe "clear/1" do
    test "removes all entries" do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(0, 0, 100)
        |> Index.add(10, 1000, 150)
        |> Index.add(20, 2000, 200)

      assert Index.size(index) == 3

      cleared = Index.clear(index)
      assert Index.size(cleared) == 0
      assert cleared.min_offset == nil
      assert cleared.max_offset == nil
    end
  end

  describe "should_index?/2" do
    test "returns true for interval offsets" do
      assert Index.should_index?(0, 0)
      assert Index.should_index?(10, 0)
      assert Index.should_index?(20, 100)
      assert Index.should_index?(100, 50)
    end

    test "returns true for large bytes written" do
      assert Index.should_index?(5, 5000)
      assert Index.should_index?(7, 4096)
    end

    test "returns false otherwise" do
      refute Index.should_index?(1, 100)
      refute Index.should_index?(5, 1000)
      refute Index.should_index?(11, 2000)
    end
  end

  describe "rebuild/2" do
    test "rebuilds index from entries list" do
      index = Index.new(@test_topic, @test_partition)

      entries = [
        {0, 0, 100},
        {10, 1000, 150},
        {20, 2000, 200}
      ]

      rebuilt = Index.rebuild(index, entries)

      assert Index.size(rebuilt) == 3
      assert {:ok, {0, 0}} = Index.lookup(rebuilt, 0)
      assert {:ok, {1000, 10}} = Index.lookup(rebuilt, 10)
      assert {:ok, {2000, 20}} = Index.lookup(rebuilt, 20)
    end
  end

  describe "stats/1" do
    test "returns statistics about the index" do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(0, 0, 100)
        |> Index.add(10, 1000, 150)
        |> Index.add(20, 2000, 200)

      stats = Index.stats(index)

      assert stats.topic == @test_topic
      assert stats.partition == @test_partition
      assert stats.entries == 3
      assert stats.min_offset == 0
      assert stats.max_offset == 20
      assert stats.memory_bytes > 0
      assert stats.coverage > 0
    end
  end

  describe "compact/2" do
    test "removes entries before retention start" do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(0, 0, 100)
        |> Index.add(10, 1000, 150)
        |> Index.add(20, 2000, 200)
        |> Index.add(30, 3000, 180)

      compacted = Index.compact(index, 15)

      # Should remove entries 0 and 10
      assert Index.size(compacted) == 2
      assert :not_found = Index.lookup(compacted, 0)
      assert :not_found = Index.lookup(compacted, 10)
      assert {:ok, {2000, 20}} = Index.lookup(compacted, 20)
      assert {:ok, {3000, 30}} = Index.lookup(compacted, 30)
      assert compacted.min_offset == 20
    end
  end

  describe "validate/1" do
    test "validates a good index" do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(0, 0, 100)
        |> Index.add(10, 1000, 150)
        |> Index.add(20, 2000, 200)

      assert {:ok, %{valid: true, entries: 3}} = Index.validate(index)
    end

    test "detects overlapping positions" do
      # Manually create an invalid index with overlapping positions
      index = Index.new(@test_topic, @test_partition)
      # Overlaps with next
      :ets.insert(index.table, {0, {0, 1500}})
      :ets.insert(index.table, {10, {1000, 150}})

      assert {:error, issues} = Index.validate(index)
      assert {:overlap, 0, 10} in issues
    end

    test "detects large gaps" do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(0, 0, 100)
        # Large gap
        |> Index.add(200, 2000, 150)

      assert {:error, issues} = Index.validate(index)
      assert {:large_gap, 0, 200} in issues
    end
  end

  describe "to_list/1" do
    test "returns all entries as list" do
      index =
        Index.new(@test_topic, @test_partition)
        |> Index.add(0, 0, 100)
        |> Index.add(10, 1000, 150)

      entries = Index.to_list(index)
      assert length(entries) == 2
      assert {0, {0, 100}} in entries
      assert {10, {1000, 150}} in entries
    end
  end
end
