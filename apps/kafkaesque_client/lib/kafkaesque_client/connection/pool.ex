defmodule KafkaesqueClient.Connection.Pool do
  @moduledoc """
  Manages a pool of connections to Kafkaesque servers with load balancing.
  """
  use GenServer
  require Logger

  alias KafkaesqueClient.Connection.Channel

  defstruct [:servers, :channels, :pool_size, :current_index]

  @type t :: %__MODULE__{
          servers: [String.t()],
          channels: [{String.t(), pid()}],
          pool_size: pos_integer(),
          current_index: non_neg_integer()
        }

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Executes a request on the next available channel (round-robin).
  """
  def execute(pool \\ __MODULE__, request, opts \\ []) do
    GenServer.call(pool, {:execute, request, opts}, 10_000)
  end

  @doc """
  Gets a channel for streaming operations.
  """
  def get_channel(pool \\ __MODULE__) do
    GenServer.call(pool, :get_channel)
  end

  @doc """
  Returns the status of all connections in the pool.
  """
  def status(pool \\ __MODULE__) do
    GenServer.call(pool, :status)
  end

  @impl true
  def init(opts) do
    servers = Keyword.get(opts, :bootstrap_servers, ["localhost:50051"])
    pool_size = Keyword.get(opts, :pool_size, 1)

    state = %__MODULE__{
      servers: servers,
      channels: [],
      pool_size: pool_size,
      current_index: 0
    }

    send(self(), :start_channels)
    {:ok, state}
  end

  @impl true
  def handle_call({:execute, request, opts}, _from, state) do
    case get_next_channel(state) do
      {:ok, server, new_state} ->
        result = Channel.call(server, request, opts)
        {:reply, result, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:get_channel, _from, state) do
    case get_next_channel(state) do
      {:ok, server, new_state} ->
        case Channel.get_channel(server) do
          {:ok, grpc_channel} -> {:reply, {:ok, grpc_channel}, new_state}
          error -> {:reply, error, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:status, _from, state) do
    status_list =
      Enum.map(state.channels, fn {server, pid} ->
        if Process.alive?(pid) do
          Channel.status(server)
        else
          %{status: :dead, server: server, pid: pid}
        end
      end)

    {:reply, status_list, state}
  end

  @impl true
  def handle_info(:start_channels, state) do
    channels =
      for server <- state.servers, _ <- 1..state.pool_size do
        case start_channel(server) do
          {:ok, pid} ->
            Process.monitor(pid)
            {server, pid}

          {:error, reason} ->
            Logger.error("Failed to start channel for #{server}: #{inspect(reason)}")
            nil
        end
      end
      |> Enum.filter(& &1)

    {:noreply, %{state | channels: channels}}
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.warning("Channel #{inspect(pid)} went down: #{inspect(reason)}")

    # Find and remove the dead channel
    {server, channels} =
      case List.keytake(state.channels, pid, 1) do
        {{server, ^pid}, remaining} -> {server, remaining}
        nil -> {nil, state.channels}
      end

    # Try to restart the channel
    new_channels =
      if server do
        case start_channel(server) do
          {:ok, new_pid} ->
            Process.monitor(new_pid)
            [{server, new_pid} | channels]

          _ ->
            channels
        end
      else
        channels
      end

    {:noreply, %{state | channels: new_channels}}
  end

  defp get_next_channel(%{channels: []} = _state) do
    {:error, :no_available_channels}
  end

  defp get_next_channel(%{channels: channels, current_index: index} = state) do
    {server, pid} = Enum.at(channels, rem(index, length(channels)))

    if Process.alive?(pid) do
      new_index = rem(index + 1, length(channels))
      {:ok, server, %{state | current_index: new_index}}
    else
      # Try next channel
      new_state = %{state | current_index: rem(index + 1, length(channels))}
      get_next_channel(new_state)
    end
  end

  defp start_channel(server) do
    DynamicSupervisor.start_child(
      KafkaesqueClient.ConnectionSupervisor,
      {Channel, server: server}
    )
  end
end
