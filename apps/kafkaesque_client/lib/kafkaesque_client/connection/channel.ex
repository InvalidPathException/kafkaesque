defmodule KafkaesqueClient.Connection.Channel do
  @moduledoc """
  Manages a single gRPC channel connection to a Kafkaesque server.
  Handles request/response, streaming, and connection health.
  """
  use GenServer
  require Logger

  alias Kafkaesque.Kafkaesque.Stub

  defstruct [:channel, :server, :status, :last_used, :in_flight, :monitor_ref]

  @type t :: %__MODULE__{
          channel: GRPC.Channel.t() | nil,
          server: String.t(),
          status: :connecting | :connected | :disconnected | :failed,
          last_used: integer(),
          in_flight: non_neg_integer(),
          monitor_ref: reference() | nil
        }

  @reconnect_interval 5_000
  @health_check_interval 30_000

  def start_link(opts) do
    server = Keyword.fetch!(opts, :server)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(server))
  end

  def call(server, request, opts \\ []) do
    GenServer.call(via_tuple(server), {:call, request, opts}, 10_000)
  end

  def get_channel(server) do
    GenServer.call(via_tuple(server), :get_channel)
  end

  def status(server) do
    GenServer.call(via_tuple(server), :status)
  end

  @impl true
  def init(opts) do
    server = Keyword.fetch!(opts, :server)

    state = %__MODULE__{
      channel: nil,
      server: server,
      status: :connecting,
      last_used: System.monotonic_time(:millisecond),
      in_flight: 0
    }

    send(self(), :connect)
    Process.send_after(self(), :health_check, @health_check_interval)

    {:ok, state}
  end

  @impl true
  def handle_call({:call, request, opts}, _from, %{status: :connected} = state) do
    # Execute synchronously for streams to work properly
    result = execute_request(state.channel, request, opts)
    {:reply, result, %{state | last_used: System.monotonic_time(:millisecond), in_flight: state.in_flight + 1}}
  end

  def handle_call({:call, _request, _opts}, _from, state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(:get_channel, _from, %{status: :connected} = state) do
    {:reply, {:ok, state.channel}, state}
  end

  def handle_call(:get_channel, _from, state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(:status, _from, state) do
    status_info = %{
      server: state.server,
      status: state.status,
      in_flight: state.in_flight,
      last_used: state.last_used
    }
    {:reply, status_info, state}
  end

  @impl true
  def handle_info(:connect, state) do
    case GRPC.Stub.connect(state.server) do
      {:ok, channel} ->
        Logger.info("Connected to Kafkaesque server: #{state.server}")
        {:noreply, %{state | channel: channel, status: :connected}}

      {:error, reason} ->
        Logger.error("Failed to connect to #{state.server}: #{inspect(reason)}")
        Process.send_after(self(), :connect, @reconnect_interval)
        {:noreply, %{state | status: :failed}}
    end
  end

  def handle_info(:health_check, %{status: :connected} = state) do
    case check_health(state.channel) do
      :ok ->
        Process.send_after(self(), :health_check, @health_check_interval)
        {:noreply, state}

      :error ->
        Logger.warning("Health check failed for #{state.server}, reconnecting...")
        send(self(), :connect)
        Process.send_after(self(), :health_check, @health_check_interval)
        {:noreply, %{state | status: :disconnected, channel: nil}}
    end
  end

  def handle_info(:health_check, state) do
    Process.send_after(self(), :health_check, @health_check_interval)
    {:noreply, state}
  end

  def handle_info({:request_complete, _ref}, state) do
    {:noreply, %{state | in_flight: max(0, state.in_flight - 1)}}
  end

  # Handle gun streaming messages - just ignore them as the stream handles them
  def handle_info({:gun_data, _conn, _stream, _fin, _data}, state) do
    {:noreply, state}
  end

  def handle_info({:gun_response, _conn, _stream, _fin, _status, _headers}, state) do
    {:noreply, state}
  end

  def handle_info({:gun_error, _conn, _stream, _reason}, state) do
    {:noreply, state}
  end

  def handle_info({:gun_trailers, _conn, _stream, _headers}, state) do
    {:noreply, state}
  end

  defp execute_request(channel, {:produce, request}, _opts) do
    Stub.produce(channel, request)
  end

  defp execute_request(channel, {:consume, request}, opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    Logger.debug("Executing consume request with timeout #{timeout}ms")
    result = Stub.consume(channel, request, timeout: timeout)
    Logger.debug("Consume request returned: #{inspect(result, limit: :infinity)}")
    result
  end

  defp execute_request(channel, {:create_topic, request}, _opts) do
    Stub.create_topic(channel, request)
  end

  defp execute_request(channel, {:list_topics, request}, _opts) do
    Stub.list_topics(channel, request)
  end

  defp execute_request(channel, {:commit_offsets, request}, _opts) do
    Stub.commit_offsets(channel, request)
  end

  defp execute_request(channel, {:get_offsets, request}, _opts) do
    Stub.get_offsets(channel, request)
  end

  defp check_health(channel) do
    case Stub.list_topics(channel, %Kafkaesque.ListTopicsRequest{}) do
      {:ok, _} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp via_tuple(server) do
    {:via, Registry, {KafkaesqueClient.ConnectionRegistry, server}}
  end
end
