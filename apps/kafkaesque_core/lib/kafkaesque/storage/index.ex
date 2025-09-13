defmodule Kafkaesque.Storage.Index do
  @moduledoc """
  ETS-based sparse index for fast offset lookups.
  Maps offset → {file_position, frame_length}.
  Rebuilt on startup from file scan.
  """

  require Logger
  alias Kafkaesque.Constants

  defstruct [:table, :topic, :partition, :min_offset, :max_offset]

  @doc """
  Creates a new index for a topic/partition.
  """
  def new(topic, partition) do
    table_name = :"index_#{topic}_#{partition}"

    table =
      case :ets.info(table_name) do
        :undefined ->
          :ets.new(table_name, [:ordered_set, :public, :named_table])

        _ ->
          :ets.delete_all_objects(table_name)
          table_name
      end

    %__MODULE__{
      table: table,
      topic: topic,
      partition: partition,
      min_offset: nil,
      max_offset: nil
    }
  end

  @doc """
  Adds an entry to the index.
  """
  def add(%__MODULE__{} = index, offset, file_position, frame_length) do
    :ets.insert(index.table, {offset, {file_position, frame_length}})

    min_offset = if is_nil(index.min_offset), do: offset, else: min(index.min_offset, offset)
    max_offset = if is_nil(index.max_offset), do: offset, else: max(index.max_offset, offset)

    %{index | min_offset: min_offset, max_offset: max_offset}
  end

  @doc """
  Looks up the file position for a given offset.
  Returns {:ok, file_position} or :not_found.
  """
  def lookup(%__MODULE__{} = index, offset) do
    cond do
      is_nil(index.min_offset) or is_nil(index.max_offset) ->
        :not_found

      offset < index.min_offset or offset > index.max_offset ->
        :not_found

      true ->
        case find_position(index.table, offset) do
          {:ok, position} -> {:ok, position}
          :not_found -> :not_found
        end
    end
  end

  @doc """
  Gets the range of offsets in the index.
  """
  def get_range(%__MODULE__{} = index) do
    {index.min_offset || 0, index.max_offset || 0}
  end

  @doc """
  Clears all entries from the index.
  """
  def clear(%__MODULE__{} = index) do
    :ets.delete_all_objects(index.table)
    %{index | min_offset: nil, max_offset: nil}
  end

  @doc """
  Returns the number of entries in the index.
  """
  def size(%__MODULE__{} = index) do
    :ets.info(index.table, :size)
  end

  @doc """
  Returns all entries in the index (for debugging).
  """
  def to_list(%__MODULE__{} = index) do
    :ets.tab2list(index.table)
  end

  @doc """
  Determines if an index entry should be added based on offset interval
  and bytes written since last index.
  """
  def should_index?(offset, bytes_written) do
    rem(offset, Constants.index_interval()) == 0 or
      bytes_written >= Constants.index_bytes_threshold()
  end

  @doc """
  Rebuilds the index from a list of offset positions.
  Used during startup scan.
  """
  def rebuild(%__MODULE__{} = index, entries) when is_list(entries) do
    cleared_index = clear(index)

    Enum.reduce(entries, cleared_index, fn {offset, position, length}, acc ->
      add(acc, offset, position, length)
    end)
  end

  @doc """
  Gets statistics about the index.
  """
  def stats(%__MODULE__{} = index) do
    entries = :ets.info(index.table, :size)
    memory = :ets.info(index.table, :memory)

    %{
      topic: index.topic,
      partition: index.partition,
      entries: entries,
      memory_words: memory,
      memory_bytes: memory * :erlang.system_info(:wordsize),
      min_offset: index.min_offset,
      max_offset: index.max_offset,
      coverage: calculate_coverage(index)
    }
  end

  defp find_position(table, offset) do
    case :ets.lookup(table, offset) do
      [{^offset, {position, _length}}] ->
        {:ok, position}

      [] ->
        find_closest_before(table, offset)
    end
  end

  defp find_closest_before(table, target_offset) do
    case :ets.prev(table, target_offset) do
      :"$end_of_table" ->
        if target_offset == 0 do
          {:ok, 0}
        else
          :not_found
        end

      prev_offset ->
        [{^prev_offset, {position, length}}] = :ets.lookup(table, prev_offset)

        offset_delta = target_offset - prev_offset

        if offset_delta == 0 do
          {:ok, position}
        else
          estimated_position = position + length
          {:ok, estimated_position}
        end
    end
  end

  defp calculate_coverage(%__MODULE__{min_offset: nil}), do: 0.0
  defp calculate_coverage(%__MODULE__{max_offset: nil}), do: 0.0

  defp calculate_coverage(%__MODULE__{} = index) do
    offset_range = index.max_offset - index.min_offset + 1

    if offset_range > 0 do
      entries = :ets.info(index.table, :size)
      Float.round(entries * 100.0 / offset_range, 2)
    else
      0.0
    end
  end

  @doc """
  Performs maintenance on the index, such as removing old entries.
  This would be called by a retention controller.
  """
  def compact(%__MODULE__{} = index, retention_start_offset) do
    match_spec = [{{:"$1", :_}, [{:<, :"$1", retention_start_offset}], [true]}]
    :ets.select_delete(index.table, match_spec)

    new_min =
      case :ets.first(index.table) do
        :"$end_of_table" -> nil
        key -> key
      end

    %{index | min_offset: new_min}
  end

  @doc """
  Validates index integrity by checking for gaps and consistency.
  Returns {:ok, stats} or {:error, issues}.
  """
  def validate(%__MODULE__{} = index) do
    entries =
      :ets.tab2list(index.table)
      |> Enum.sort_by(&elem(&1, 0))

    issues = find_issues(entries)

    if Enum.empty?(issues) do
      {:ok, %{valid: true, entries: length(entries)}}
    else
      {:error, issues}
    end
  end

  defp find_issues(entries) do
    entries
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.reduce([], fn [{offset1, {pos1, len1}}, {offset2, {pos2, _len2}}], acc ->
      cond do
        pos1 + len1 > pos2 ->
          [{:overlap, offset1, offset2} | acc]

        offset2 - offset1 > 100 ->
          [{:large_gap, offset1, offset2} | acc]

        true ->
          acc
      end
    end)
    |> Enum.reverse()
  end
end
