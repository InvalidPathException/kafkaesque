defmodule KafkaesqueClient.Record do
  @moduledoc """
  Record types for producers and consumers, mirroring Kafka's ProducerRecord and ConsumerRecord.
  """

  defmodule ProducerRecord do
    @moduledoc """
    A record to be sent to Kafkaesque, similar to Kafka's ProducerRecord.
    """
    @type t :: %__MODULE__{
            topic: String.t(),
            partition: integer() | nil,
            key: binary() | nil,
            value: binary(),
            headers: %{String.t() => binary()},
            timestamp: integer() | nil
          }

    defstruct [:topic, :partition, :key, :value, headers: %{}, timestamp: nil]

    @doc """
    Creates a new ProducerRecord with minimal required fields.
    """
    def new(topic, value) when is_binary(topic) and is_binary(value) do
      %__MODULE__{topic: topic, value: value}
    end

    def new(topic, key, value) when is_binary(topic) and is_binary(value) do
      %__MODULE__{topic: topic, key: key, value: value}
    end

    @doc """
    Creates a ProducerRecord from a map or keyword list.
    """
    def from_map(params) when is_map(params) do
      struct(__MODULE__, params)
    end

    def from_map(params) when is_list(params) do
      struct(__MODULE__, Map.new(params))
    end

    @doc """
    Converts to the protobuf Record format.
    """
    def to_proto(%__MODULE__{} = record) do
      headers =
        Enum.map(record.headers, fn {k, v} ->
          %Kafkaesque.RecordHeader{key: k, value: v}
        end)

      %Kafkaesque.Record{
        key: record.key || <<>>,
        value: record.value,
        headers: headers,
        timestamp_ms: record.timestamp || System.system_time(:millisecond)
      }
    end
  end

  defmodule ConsumerRecord do
    @moduledoc """
    A record consumed from Kafkaesque, similar to Kafka's ConsumerRecord.
    """
    @type t :: %__MODULE__{
            topic: String.t(),
            partition: integer(),
            offset: integer(),
            key: binary() | nil,
            value: binary(),
            headers: %{String.t() => binary()},
            timestamp: integer(),
            timestamp_type: :create_time | :log_append_time
          }

    defstruct [
      :topic,
      :partition,
      :offset,
      :key,
      :value,
      :timestamp,
      headers: %{},
      timestamp_type: :create_time
    ]

    @doc """
    Creates a ConsumerRecord from a protobuf FetchedBatch and Record.
    """
    def from_proto(%Kafkaesque.FetchedBatch{} = batch, %Kafkaesque.Record{} = record, offset) do
      headers =
        Map.new(record.headers, fn %Kafkaesque.RecordHeader{key: k, value: v} ->
          {k, v}
        end)

      %__MODULE__{
        topic: batch.topic,
        partition: batch.partition,
        offset: offset,
        key: if(record.key == <<>>, do: nil, else: record.key),
        value: record.value,
        headers: headers,
        timestamp: record.timestamp_ms,
        timestamp_type: :create_time
      }
    end

    @doc """
    Creates multiple ConsumerRecords from a FetchedBatch.
    """
    def from_batch(%Kafkaesque.FetchedBatch{} = batch) do
      batch.records
      |> Enum.with_index()
      |> Enum.map(fn {record, idx} ->
        from_proto(batch, record, batch.base_offset + idx)
      end)
    end
  end

  defmodule RecordMetadata do
    @moduledoc """
    Metadata about a record that was successfully produced.
    """
    @type t :: %__MODULE__{
            topic: String.t(),
            partition: integer(),
            offset: integer(),
            timestamp: integer() | nil
          }

    defstruct [:topic, :partition, :offset, :timestamp]

    @doc """
    Creates RecordMetadata from a ProduceResponse.
    """
    def from_produce_response(%Kafkaesque.ProduceResponse{} = resp, index \\ 0) do
      %__MODULE__{
        topic: resp.topic,
        partition: resp.partition,
        offset: resp.base_offset + index,
        timestamp: nil
      }
    end
  end

  defmodule TopicPartition do
    @moduledoc """
    Represents a topic and partition pair.
    """
    @type t :: %__MODULE__{
            topic: String.t(),
            partition: integer()
          }

    defstruct [:topic, :partition]

    def new(topic, partition \\ 0) do
      %__MODULE__{topic: topic, partition: partition}
    end
  end

  defmodule OffsetAndMetadata do
    @moduledoc """
    Represents an offset with optional metadata for commit operations.
    """
    @type t :: %__MODULE__{
            offset: integer(),
            metadata: String.t() | nil
          }

    defstruct [:offset, metadata: nil]

    def new(offset, metadata \\ nil) do
      %__MODULE__{offset: offset, metadata: metadata}
    end
  end
end
