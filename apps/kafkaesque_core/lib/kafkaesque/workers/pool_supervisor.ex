defmodule Kafkaesque.Workers.PoolSupervisor do
  @moduledoc """
  Supervisor for worker pools.
  Manages poolboy pools for CPU-intensive tasks.

  Currently manages:
  - Compression worker pool

  Future pools:
  - Encryption workers
  - Serialization workers
  """

  use Supervisor
  require Logger

  alias Kafkaesque.Workers.CompressionWorker

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Perform compression using the pool.
  """
  def compress(data, algorithm \\ :gzip) do
    :poolboy.transaction(:compression_pool, fn worker ->
      CompressionWorker.compress(worker, data, algorithm)
    end, 30_000)
  end

  @doc """
  Perform decompression using the pool.
  """
  def decompress(data, algorithm \\ :gzip) do
    :poolboy.transaction(:compression_pool, fn worker ->
      CompressionWorker.decompress(worker, data, algorithm)
    end, 30_000)
  end

  @doc """
  Compress multiple items in batch.
  """
  def compress_batch(items, algorithm \\ :gzip) do
    :poolboy.transaction(:compression_pool, fn worker ->
      CompressionWorker.compress_batch(worker, items, algorithm)
    end, 60_000)
  end

  @doc """
  Check pool status.
  """
  def pool_status do
    # poolboy.status returns a tuple {state, available, overflow, monitors}
    # where monitors is the count, not a list
    case :poolboy.status(:compression_pool) do
      {state, available, overflow, monitor_count} when is_atom(state) and is_integer(monitor_count) ->
        %{
          state: state,
          available_workers: available,
          overflow: overflow,
          monitors: monitor_count,
          healthy: state == :ready
        }
    end
  catch
    _kind, _reason -> %{error: "Pool not available"}
  end

  @impl true
  def init(_opts) do
    # Pool configuration based on system schedulers
    pool_size = System.schedulers_online()
    max_overflow = max(2, div(pool_size, 2))

    Logger.info("Starting worker pools with size=#{pool_size}, max_overflow=#{max_overflow}")

    pool_config = [
      name: {:local, :compression_pool},
      worker_module: CompressionWorker,
      size: pool_size,
      max_overflow: max_overflow,
      strategy: :fifo  # First-in-first-out for fairness
    ]

    children = [
      :poolboy.child_spec(:compression_pool, pool_config, [])
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
