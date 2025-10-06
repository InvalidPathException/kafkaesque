defmodule Kafkaesque.Partition.Router do
  @moduledoc """
  Centralized partition routing logic.

  * Uses consistent hashing when a key is provided so identical keys land on the
    same partition.
  * Falls back to round-robin distribution for `nil` keys to spread load.
  * If the router process is unavailable, defaults to a random partition.
  """

  use GenServer

  require Logger

  @name __MODULE__

  ## Client API

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @name)
  end

  @doc """
  Choose a partition based on the provided key and number of partitions.
  """
  @spec route(binary() | nil | term(), pos_integer()) :: non_neg_integer()
  def route(_key, num_partitions) when num_partitions <= 0, do: 0

  def route(key, num_partitions) when is_binary(key) and num_partitions > 0 do
    :erlang.phash2(key, num_partitions)
  end

  def route(nil, num_partitions) when num_partitions > 0 do
    call_round_robin(num_partitions)
  end

  def route(_key, num_partitions) when num_partitions > 0 do
    :rand.uniform(num_partitions) - 1
  end

  ## GenServer callbacks

  @impl true
  def init(_opts) do
    {:ok, %{counter: 0}}
  end

  @impl true
  def handle_call({:round_robin, partitions}, _from, %{counter: counter} = state) do
    partition = rem(counter, partitions)
    {:reply, partition, %{state | counter: counter + 1}}
  end

  ## Helpers

  defp call_round_robin(partitions) do
    case GenServer.whereis(@name) do
      nil ->
        Logger.warning("Partition router not started, using random fallback")
        :rand.uniform(partitions) - 1

      pid ->
        GenServer.call(pid, {:round_robin, partitions})
    end
  catch
    :exit, reason ->
      Logger.warning("Partition router call failed: #{inspect(reason)}")
      :rand.uniform(partitions) - 1
  end
end
