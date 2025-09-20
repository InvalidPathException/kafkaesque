defmodule KafkaesqueDashboard.MetricsCache do
  @moduledoc """
  GenServer that caches consumer group data to avoid blocking I/O in LiveView.
  Updates the cache periodically in the background.
  """

  use GenServer
  require Logger

  alias Kafkaesque.Storage.SingleFile

  @refresh_interval 5_000  # 5 seconds

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_consumer_groups do
    GenServer.call(__MODULE__, :get_consumer_groups)
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    # Schedule first refresh
    Process.send_after(self(), :refresh, 100)

    state = %{
      consumer_groups: [],
      last_updated: nil
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:get_consumer_groups, _from, state) do
    {:reply, state.consumer_groups, state}
  end

  @impl true
  def handle_info(:refresh, state) do
    # Refresh consumer groups data in background
    groups = fetch_consumer_groups()

    # Schedule next refresh
    Process.send_after(self(), :refresh, @refresh_interval)

    {:noreply, %{state | consumer_groups: groups, last_updated: DateTime.utc_now()}}
  end

  # Private Functions

  defp fetch_consumer_groups do
    offsets_dir = Application.get_env(:kafkaesque_core, :offsets_dir, "./offsets")

    case File.ls(offsets_dir) do
      {:ok, files} ->
        # Parse DETS files to find unique consumer groups
        groups =
          files
          |> Enum.filter(&String.ends_with?(&1, "_offsets.dets"))
          |> Enum.flat_map(fn file ->
            extract_consumer_groups_from_file(Path.join(offsets_dir, file))
          end)
          |> Enum.group_by(& &1.name)
          |> Enum.map(fn {group_name, group_data} ->
            # Aggregate data for each group
            total_lag = Enum.reduce(group_data, 0, fn g, acc -> acc + g.lag end)

            %{
              name: group_name,
              lag: total_lag,
              members: length(group_data),
              state: "stable"
            }
          end)

        if groups == [] do
          []
        else
          groups
        end

      {:error, reason} ->
        Logger.warning("Failed to read offsets directory: #{inspect(reason)}")
        []
    end
  end

  defp extract_consumer_groups_from_file(file_path) do
    # Parse filename to get topic and partition
    basename = Path.basename(file_path, "_offsets.dets")

    case String.split(basename, "_") do
      [topic | rest] when rest != [] ->
        partition_str = List.last(rest)
        partition = String.to_integer(partition_str || "0")
        topic_name = Enum.join([topic | Enum.drop(rest, -1)], "_")

        # Use a fixed reference for DETS operations
        table_ref = make_ref()

        case :dets.open_file(table_ref, [{:file, String.to_charlist(file_path)}, {:access, :read}]) do
          {:ok, _} ->
            groups = extract_groups_from_table(table_ref, topic_name, partition)
            :dets.close(table_ref)
            groups

          {:error, reason} ->
            Logger.debug("Failed to open DETS file #{file_path}: #{inspect(reason)}")
            []
        end

      _ ->
        []
    end
  end

  defp extract_groups_from_table(table_ref, topic, partition) do
    # Read all entries from DETS table
    case :dets.match_object(table_ref, :_) do
      entries when is_list(entries) ->
        entries
        |> Enum.map(fn
          {{^topic, ^partition, group}, offset} ->
            # Calculate lag by comparing with latest offset
            latest = get_latest_offset(topic, partition)
            lag = max(0, latest - offset)
            %{name: group, lag: lag, topic: topic, partition: partition}

          _ ->
            nil
        end)
        |> Enum.reject(&is_nil/1)

      _ ->
        []
    end
  end

  defp get_latest_offset(topic, partition) do
    case SingleFile.get_offsets(topic, partition) do
      {:ok, %{latest: latest}} -> latest
      _ -> 0
    end
  end
end
