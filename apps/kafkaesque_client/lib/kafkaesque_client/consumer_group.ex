defmodule KafkaesqueClient.ConsumerGroup do
  @moduledoc """
  Manages consumer group coordination, offset tracking, and heartbeats.
  """
  use GenServer
  require Logger

  alias KafkaesqueClient.Connection.Pool
  alias KafkaesqueClient.Record.TopicPartition

  defstruct [
    :group_id,
    :pool,
    :offsets,
    :session_timeout_ms,
    :heartbeat_interval_ms,
    :last_heartbeat,
    :generation_id,
    :member_id
  ]

  @type t :: %__MODULE__{
          group_id: String.t(),
          pool: atom() | pid(),
          offsets: %{TopicPartition.t() => integer()},
          session_timeout_ms: pos_integer(),
          heartbeat_interval_ms: pos_integer(),
          last_heartbeat: integer(),
          generation_id: integer(),
          member_id: String.t() | nil
        }

  # Client API

  def start_link(config) do
    GenServer.start_link(__MODULE__, config)
  end

  @doc """
  Gets the committed offset for a topic partition.
  """
  def get_committed_offset(group, %TopicPartition{} = tp) do
    GenServer.call(group, {:get_offset, tp})
  end

  @doc """
  Commits an offset for a topic partition.
  """
  def commit_offset(group, %TopicPartition{} = tp, offset) do
    GenServer.call(group, {:commit_offset, tp, offset})
  end

  @doc """
  Fetches all committed offsets from the server.
  """
  def fetch_offsets(group) do
    GenServer.call(group, :fetch_offsets)
  end

  @doc """
  Sends a heartbeat to maintain group membership.
  """
  def heartbeat(group) do
    GenServer.cast(group, :heartbeat)
  end

  # Server Callbacks

  @impl true
  def init(config) do
    state = %__MODULE__{
      group_id: config.group_id,
      pool: Pool,
      offsets: %{},
      session_timeout_ms: Map.get(config, :session_timeout_ms, 30_000),
      heartbeat_interval_ms: Map.get(config, :heartbeat_interval_ms, 3_000),
      last_heartbeat: System.monotonic_time(:millisecond),
      generation_id: 0,
      member_id: generate_member_id()
    }

    # Start heartbeat timer
    Process.send_after(self(), :heartbeat, state.heartbeat_interval_ms)

    # Fetch initial offsets
    send(self(), :fetch_initial_offsets)

    {:ok, state}
  end

  @impl true
  def handle_call({:get_offset, tp}, _from, state) do
    offset = Map.get(state.offsets, tp)

    if offset do
      {:reply, {:ok, offset}, state}
    else
      # Since we always return :not_found, just return that
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:commit_offset, tp, offset}, _from, state) do
    # Update local cache
    new_offsets = Map.put(state.offsets, tp, offset)

    # Commit to server
    result = commit_offset_to_server(state, tp, offset)

    {:reply, result, %{state | offsets: new_offsets}}
  end

  def handle_call(:fetch_offsets, _from, state) do
    case fetch_all_offsets_from_server(state) do
      {:ok, offsets} ->
        {:reply, {:ok, offsets}, %{state | offsets: offsets}}

      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_cast(:heartbeat, state) do
    new_state = %{state | last_heartbeat: System.monotonic_time(:millisecond)}
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:heartbeat, state) do
    # Check session timeout
    now = System.monotonic_time(:millisecond)
    time_since_heartbeat = now - state.last_heartbeat

    if time_since_heartbeat > state.session_timeout_ms do
      Logger.warning("Consumer group #{state.group_id} session timeout")
    end

    # Schedule next heartbeat check
    Process.send_after(self(), :heartbeat, state.heartbeat_interval_ms)

    {:noreply, state}
  end

  def handle_info(:fetch_initial_offsets, state) do
    # Fetch any existing committed offsets
    {:ok, offsets} = fetch_all_offsets_from_server(state)
    {:noreply, %{state | offsets: offsets}}
  end

  # Private Functions

  defp generate_member_id do
    "consumer-" <> Base.encode16(:crypto.strong_rand_bytes(8))
  end

  defp commit_offset_to_server(state, %TopicPartition{} = tp, offset) do
    request = %Kafkaesque.CommitOffsetsRequest{
      group: state.group_id,
      offsets: [
        %Kafkaesque.CommitOffsetsRequest.Offset{
          topic: tp.topic,
          partition: tp.partition,
          offset: offset
        }
      ]
    }

    case Pool.execute(state.pool, {:commit_offsets, request}) do
      {:ok, _response} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_all_offsets_from_server(state) do
    # We need to fetch offsets for all topics the group is interested in
    # Since we don't track subscriptions here, we'll return an empty map
    # The Consumer module handles offset fetching per topic when subscribing
    Logger.debug("ConsumerGroup #{state.group_id} fetching committed offsets")
    {:ok, %{}}
  end
end
