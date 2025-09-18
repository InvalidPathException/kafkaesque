defmodule KafkaesqueClient.Config do
  @moduledoc """
  Configuration for Kafkaesque client connections, producers, and consumers.
  """

  @type t :: %__MODULE__{
          bootstrap_servers: [String.t()],
          connection_timeout_ms: non_neg_integer(),
          request_timeout_ms: non_neg_integer(),
          retry_backoff_ms: non_neg_integer(),
          max_retries: non_neg_integer(),
          pool_size: pos_integer(),
          metadata: map()
        }

  defstruct bootstrap_servers: ["localhost:50051"],
            connection_timeout_ms: 5_000,
            request_timeout_ms: 30_000,
            retry_backoff_ms: 100,
            max_retries: 3,
            pool_size: 5,
            metadata: %{}

  @type producer_config :: %{
          required(:bootstrap_servers) => [String.t()],
          optional(:acks) => :none | :leader | :all,
          optional(:batch_size) => pos_integer(),
          optional(:linger_ms) => non_neg_integer(),
          optional(:compression_type) => :none | :gzip | :snappy,
          optional(:max_in_flight_requests) => pos_integer(),
          optional(:retry_backoff_ms) => non_neg_integer(),
          optional(:max_retries) => non_neg_integer()
        }

  @type consumer_config :: %{
          required(:bootstrap_servers) => [String.t()],
          required(:group_id) => String.t(),
          optional(:auto_offset_reset) => :earliest | :latest,
          optional(:enable_auto_commit) => boolean(),
          optional(:auto_commit_interval_ms) => pos_integer(),
          optional(:session_timeout_ms) => pos_integer(),
          optional(:heartbeat_interval_ms) => pos_integer(),
          optional(:max_poll_records) => pos_integer(),
          optional(:max_poll_interval_ms) => pos_integer(),
          optional(:fetch_min_bytes) => pos_integer(),
          optional(:fetch_max_wait_ms) => pos_integer()
        }

  @doc """
  Creates a new configuration from options.
  """
  def new(opts \\ []) do
    struct(__MODULE__, opts)
  end

  @doc """
  Validates and normalizes producer configuration.
  """
  def validate_producer_config(config) when is_map(config) do
    defaults = %{
      acks: :leader,
      batch_size: 16_384,
      linger_ms: 100,
      compression_type: :none,
      max_in_flight_requests: 5,
      retry_backoff_ms: 100,
      max_retries: 3
    }

    config = Map.merge(defaults, config)

    with :ok <- validate_bootstrap_servers(config.bootstrap_servers),
         :ok <- validate_acks(config.acks) do
      {:ok, config}
    end
  end

  @doc """
  Validates and normalizes consumer configuration.
  """
  def validate_consumer_config(config) when is_map(config) do
    defaults = %{
      auto_offset_reset: :latest,
      enable_auto_commit: true,
      auto_commit_interval_ms: 5_000,
      session_timeout_ms: 30_000,
      heartbeat_interval_ms: 3_000,
      max_poll_records: 500,
      max_poll_interval_ms: 300_000,
      fetch_min_bytes: 1,
      fetch_max_wait_ms: 2000
    }

    config = Map.merge(defaults, config)

    with :ok <- validate_bootstrap_servers(config.bootstrap_servers),
         :ok <- validate_group_id(config.group_id),
         :ok <- validate_offset_reset(config.auto_offset_reset) do
      {:ok, config}
    end
  end

  defp validate_bootstrap_servers([_ | _] = servers) do
    if Enum.all?(servers, &valid_server_address?/1) do
      :ok
    else
      {:error, :invalid_bootstrap_servers}
    end
  end

  defp validate_bootstrap_servers(_), do: {:error, :invalid_bootstrap_servers}

  defp validate_group_id(nil), do: {:error, :missing_group_id}
  defp validate_group_id(""), do: {:error, :invalid_group_id}
  defp validate_group_id(_), do: :ok

  defp validate_acks(acks) when acks in [:none, :leader, :all], do: :ok
  defp validate_acks(_), do: {:error, :invalid_acks}

  defp validate_offset_reset(reset) when reset in [:earliest, :latest], do: :ok
  defp validate_offset_reset(_), do: {:error, :invalid_offset_reset}

  defp valid_server_address?(address) when is_binary(address) do
    case String.split(address, ":") do
      [_host, port] -> valid_port?(port)
      [_host] -> true
      _ -> false
    end
  end

  defp valid_server_address?(_), do: false

  defp valid_port?(port) do
    case Integer.parse(port) do
      {p, ""} when p > 0 and p <= 65_535 -> true
      _ -> false
    end
  end
end
