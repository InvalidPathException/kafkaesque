defmodule Kafkaesque.Test.MockStream do
  @moduledoc """
  Mock GRPC stream for testing streaming responses.
  """

  defstruct [
    :pid,
    :messages,
    :closed,
    :adapter_payload
  ]

  @doc """
  Create a new mock stream.
  """
  def new(opts \\ []) do
    pid = Keyword.get(opts, :pid, self())

    %__MODULE__{
      pid: pid,
      messages: [],
      closed: false,
      adapter_payload: %{pid: pid}
    }
  end

  @doc """
  Get all messages sent to the stream.
  """
  def get_messages(%__MODULE__{} = stream) do
    GenServer.call(stream.pid, :get_messages)
  end

  @doc """
  Check if stream is closed.
  """
  def closed?(%__MODULE__{} = stream) do
    GenServer.call(stream.pid, :is_closed)
  end

  @doc """
  Start a mock stream server for testing.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__.Server, opts)
  end

  defmodule Server do
    @moduledoc false
    use GenServer

    def init(_opts) do
      {:ok, %{messages: [], closed: false}}
    end

    def handle_call(:get_messages, _from, state) do
      {:reply, Enum.reverse(state.messages), state}
    end

    def handle_call(:is_closed, _from, state) do
      {:reply, state.closed, state}
    end

    def handle_call({:send_reply, message}, _from, state) do
      new_state = %{state | messages: [message | state.messages]}
      {:reply, :ok, new_state}
    end

    def handle_call(:close, _from, state) do
      {:reply, :ok, %{state | closed: true}}
    end
  end
end

defmodule GRPC.Server.MockAdapter do
  @moduledoc """
  Mock adapter for GRPC.Server functions used in tests.
  """

  def send_reply(%Kafkaesque.Test.MockStream{} = stream, message) do
    if stream.pid && Process.alive?(stream.pid) do
      GenServer.call(stream.pid, {:send_reply, message})
    else
      {:error, :stream_closed}
    end
  end

  def send_reply(_stream, _message) do
    # For non-mock streams, just return ok
    :ok
  end
end
