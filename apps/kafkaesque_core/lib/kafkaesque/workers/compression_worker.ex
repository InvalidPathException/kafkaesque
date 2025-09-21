defmodule Kafkaesque.Workers.CompressionWorker do
  @moduledoc """
  Worker process for CPU-intensive compression/decompression tasks.
  Runs in a poolboy-managed pool to prevent blocking main pipeline.

  Supports multiple compression algorithms:
  - gzip (via :zlib)
  - Future: snappy, lz4, zstd
  """

  use GenServer
  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil)
  end

  @doc """
  Compress data using specified algorithm.
  """
  def compress(worker, data, algorithm \\ :gzip) do
    GenServer.call(worker, {:compress, data, algorithm}, 30_000)
  end

  @doc """
  Decompress data using specified algorithm.
  """
  def decompress(worker, data, algorithm \\ :gzip) do
    GenServer.call(worker, {:decompress, data, algorithm}, 30_000)
  end

  @doc """
  Batch compress multiple items.
  """
  def compress_batch(worker, items, algorithm \\ :gzip) do
    GenServer.call(worker, {:compress_batch, items, algorithm}, 60_000)
  end

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:compress, data, :gzip}, _from, state) do
    compressed = :zlib.gzip(data)
    ratio = byte_size(compressed) / byte_size(data)
    {:reply, {:ok, compressed, %{ratio: ratio, algorithm: :gzip}}, state}
  rescue
    error ->
      {:reply, {:error, error}, state}
  end

  def handle_call({:compress, data, :none}, _from, state) do
    {:reply, {:ok, data, %{ratio: 1.0, algorithm: :none}}, state}
  end

  def handle_call({:compress, _data, algorithm}, _from, state) do
    {:reply, {:error, {:unsupported_algorithm, algorithm}}, state}
  end

  @impl true
  def handle_call({:decompress, data, :gzip}, _from, state) do
    decompressed = :zlib.gunzip(data)
    {:reply, {:ok, decompressed}, state}
  rescue
    error ->
      {:reply, {:error, error}, state}
  end

  def handle_call({:decompress, data, :none}, _from, state) do
    {:reply, {:ok, data}, state}
  end

  def handle_call({:decompress, _data, algorithm}, _from, state) do
    {:reply, {:error, {:unsupported_algorithm, algorithm}}, state}
  end

  @impl true
  def handle_call({:compress_batch, items, algorithm}, _from, state) do
    results = Enum.map(items, fn item ->
      case do_compress(item, algorithm) do
        {:ok, compressed, meta} ->
          {:ok, %{original: item, compressed: compressed, meta: meta}}
        {:error, reason} ->
          {:error, reason}
      end
    end)

    successful = Enum.filter(results, &match?({:ok, _}, &1))
    failed = Enum.filter(results, &match?({:error, _}, &1))

    {:reply, {:ok, %{successful: successful, failed: failed}}, state}
  end

  defp do_compress(data, :gzip) do
    compressed = :zlib.gzip(data)
    ratio = byte_size(compressed) / byte_size(data)
    {:ok, compressed, %{ratio: ratio, algorithm: :gzip}}
  rescue
    error -> {:error, error}
  end

  defp do_compress(data, :none) do
    {:ok, data, %{ratio: 1.0, algorithm: :none}}
  end

  defp do_compress(_data, algorithm) do
    {:error, {:unsupported_algorithm, algorithm}}
  end
end
