defmodule KafkaesqueClient.Consumer.StreamCoordinatorTest do
  use ExUnit.Case, async: false

  alias Kafkaesque.DescribeTopicResponse
  alias Kafkaesque.PartitionInfo
  alias KafkaesqueClient.Consumer.StreamCoordinator
  alias KafkaesqueClient.Record.TopicPartition

  defmodule FakePool do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    def set_channel_reply(pool, reply) do
      GenServer.cast(pool, {:set_channel_reply, reply})
    end

    @impl true
    def init(opts) do
      state = %{
        describe_topics: Keyword.get(opts, :describe_topics, %{}),
        get_offsets: Keyword.get(opts, :get_offsets, %{}),
        get_channel_reply: Keyword.get(opts, :get_channel_reply, {:error, :no_channel})
      }

      {:ok, state}
    end

    @impl true
    def handle_call({:execute, {:describe_topic, request}, _opts}, _from, state) do
      case Map.fetch(state.describe_topics, request.topic) do
        {:ok, response} -> {:reply, {:ok, response}, state}
        :error -> {:reply, {:error, :unknown_topic}, state}
      end
    end

    def handle_call({:execute, {:get_offsets, request}, _opts}, _from, state) do
      key = {request.topic, request.partition}

      case Map.fetch(state.get_offsets, key) do
        {:ok, response} -> {:reply, {:ok, response}, state}
        :error -> {:reply, {:error, :unknown_partition}, state}
      end
    end

    def handle_call(:get_channel, _from, state) do
      {:reply, state.get_channel_reply, state}
    end

    @impl true
    def handle_cast({:set_channel_reply, reply}, state) do
      {:noreply, %{state | get_channel_reply: reply}}
    end
  end

  defmodule FakeStub do
    def consume(_channel, _request, _opts), do: {:error, :not_implemented}
  end

  setup do
    topic = "test-topic-#{System.unique_integer([:positive])}"

    describe_response = %DescribeTopicResponse{
      topic: topic,
      partitions: 1,
      retention_hours: 24,
      created_at_ms: 1_700_000_000_000,
      partition_infos: [
        %PartitionInfo{partition: 0, earliest_offset: 0, latest_offset: 0, size_bytes: 0}
      ]
    }

    {:ok, pool} =
      FakePool.start_link(
        describe_topics: %{topic => describe_response},
        get_channel_reply: {:error, :no_channel}
      )

    config = %{fetch_max_wait_ms: 50}

    {:ok, coordinator} =
      StreamCoordinator.start_link(
        pool: pool,
        config: config,
        group_id: "test-group",
        owner: self(),
        stub: FakeStub
      )

    on_exit(fn -> StreamCoordinator.stop(coordinator) end)

    {:ok, coordinator: coordinator, pool: pool, topic: topic}
  end

  test "assign stores positions and schedules restart on failure", %{coordinator: coordinator} do
    tp = TopicPartition.new("topic-assign", 0)

    :ok = StreamCoordinator.assign(coordinator, [tp], %{tp => 5})

    state =
      eventually(coordinator, fn state ->
        MapSet.member?(state.assignments, tp) and Map.get(state.positions, tp) == 5 and
          Map.get(state.restart_attempts, tp, 0) >= 1
      end)

    assert Map.has_key?(state.restart_timers, tp)
  end

  test "pause cancels outstanding restarts and resume restarts streams", %{
    coordinator: coordinator
  } do
    tp = TopicPartition.new("topic-pause", 0)

    :ok = StreamCoordinator.assign(coordinator, [tp], %{tp => 0})
    eventually(coordinator, fn state -> Map.get(state.restart_attempts, tp, 0) >= 1 end)

    :ok = StreamCoordinator.pause(coordinator, [tp])

    state =
      eventually(coordinator, fn state ->
        MapSet.member?(state.paused, tp) and
          not Map.has_key?(state.restart_timers, tp) and
          Map.get(state.restart_attempts, tp) == nil
      end)

    assert MapSet.member?(state.paused, tp)

    :ok = StreamCoordinator.resume(coordinator, [tp])

    resume_state =
      eventually(coordinator, fn resume_state ->
        not MapSet.member?(resume_state.paused, tp) and
          Map.get(resume_state.restart_attempts, tp, 0) >= 1
      end)

    refute MapSet.member?(resume_state.paused, tp)
  end

  test "unsubscribe clears assignments, positions, and cached metadata", %{
    coordinator: coordinator,
    topic: topic
  } do
    tp = TopicPartition.new(topic, 0)

    assert {:ok, [metadata]} = StreamCoordinator.subscribe(coordinator, [topic])
    assert metadata.name == topic

    :ok = StreamCoordinator.assign(coordinator, [tp], %{tp => 0})
    eventually(coordinator, fn state -> MapSet.member?(state.assignments, tp) end)

    :ok = StreamCoordinator.unsubscribe(coordinator, [topic])

    state =
      eventually(coordinator, fn state ->
        MapSet.size(state.assignments) == 0 and
          map_size(state.positions) == 0 and
          map_size(state.metadata_cache) == 0
      end)

    assert MapSet.equal?(state.subscriptions, MapSet.new())
    assert MapSet.equal?(state.paused, MapSet.new())
  end

  test "repeated stream errors increase restart attempts", %{coordinator: coordinator} do
    tp = TopicPartition.new("topic-errors", 0)

    :ok = StreamCoordinator.assign(coordinator, [tp], %{tp => 0})

    eventually(coordinator, fn state -> Map.get(state.restart_attempts, tp, 0) >= 1 end)

    send(coordinator, {:stream_error, tp, :temporary_failure})

    state = eventually(coordinator, fn state -> Map.get(state.restart_attempts, tp, 0) >= 2 end)

    assert Map.get(state.restart_attempts, tp, 0) >= 2
  end

  defp eventually(coordinator, predicate, attempts \\ 50)

  defp eventually(_coordinator, _predicate, 0), do: flunk("condition not met in time")

  defp eventually(coordinator, predicate, attempts) do
    state = :sys.get_state(coordinator)

    if predicate.(state) do
      state
    else
      Process.sleep(10)
      eventually(coordinator, predicate, attempts - 1)
    end
  end
end
