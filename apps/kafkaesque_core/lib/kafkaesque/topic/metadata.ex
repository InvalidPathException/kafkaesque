defmodule Kafkaesque.Topic.Metadata do
  @moduledoc """
  In-memory metadata registry for topics.
  Stores per-topic attributes needed for discovery APIs like DescribeTopic.
  """

  use GenServer

  @table :kafkaesque_topic_metadata

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Upserts metadata for the given topic.
  """
  @spec put(String.t(), map()) :: :ok
  def put(topic, attrs) when is_binary(topic) and is_map(attrs) do
    GenServer.call(__MODULE__, {:put, topic, attrs})
  end

  @doc """
  Fetches metadata for a topic.
  """
  @spec get(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get(topic) when is_binary(topic) do
    case :ets.lookup(@table, topic) do
      [{^topic, attrs}] -> {:ok, attrs}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Deletes metadata for a topic.
  """
  @spec delete(String.t()) :: :ok
  def delete(topic) when is_binary(topic) do
    GenServer.call(__MODULE__, {:delete, topic})
  end

  ## GenServer callbacks

  @impl true
  def init(_opts) do
    table =
      :ets.new(@table, [
        :named_table,
        :set,
        :protected,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:put, topic, attrs}, _from, state) do
    true = :ets.insert(@table, {topic, attrs})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete, topic}, _from, state) do
    :ets.delete(@table, topic)
    {:reply, :ok, state}
  end
end
