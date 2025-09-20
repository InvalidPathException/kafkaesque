defmodule Kafkaesque.ProduceRequest.Acks do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:ACKS_NONE, 0)
  field(:ACKS_LEADER, 1)
end

defmodule Kafkaesque.Topic do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:name, 1, type: :string)
  field(:partitions, 2, type: :int32)
end

defmodule Kafkaesque.RecordHeader do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :bytes)
end

defmodule Kafkaesque.Record do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:key, 1, type: :bytes)
  field(:value, 2, type: :bytes)
  field(:headers, 3, repeated: true, type: Kafkaesque.RecordHeader)
  field(:timestamp_ms, 4, type: :int64, json_name: "timestampMs")
end

defmodule Kafkaesque.ProduceRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:topic, 1, type: :string)
  field(:partition, 2, type: :int32)
  field(:records, 3, repeated: true, type: Kafkaesque.Record)
  field(:acks, 4, type: Kafkaesque.ProduceRequest.Acks, enum: true)
  field(:max_batch_bytes, 5, type: :int32, json_name: "maxBatchBytes")
end

defmodule Kafkaesque.ProduceResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:topic, 1, type: :string)
  field(:partition, 2, type: :int32)
  field(:base_offset, 3, type: :int64, json_name: "baseOffset")
  field(:count, 4, type: :int32)
end

defmodule Kafkaesque.ConsumeRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:topic, 1, type: :string)
  field(:partition, 2, type: :int32)
  field(:group, 3, type: :string)
  field(:offset, 4, type: :int64)
  field(:max_bytes, 5, type: :int32, json_name: "maxBytes")
  field(:max_wait_ms, 6, type: :int32, json_name: "maxWaitMs")
  field(:auto_commit, 7, type: :bool, json_name: "autoCommit")
end

defmodule Kafkaesque.FetchedBatch do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:topic, 1, type: :string)
  field(:partition, 2, type: :int32)
  field(:high_watermark, 3, type: :int64, json_name: "highWatermark")
  field(:base_offset, 4, type: :int64, json_name: "baseOffset")
  field(:records, 5, repeated: true, type: Kafkaesque.Record)
end

defmodule Kafkaesque.CommitOffsetsRequest.Offset do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:topic, 1, type: :string)
  field(:partition, 2, type: :int32)
  field(:offset, 3, type: :int64)
end

defmodule Kafkaesque.CommitOffsetsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:group, 1, type: :string)
  field(:offsets, 2, repeated: true, type: Kafkaesque.CommitOffsetsRequest.Offset)
end

defmodule Kafkaesque.CommitOffsetsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3
end

defmodule Kafkaesque.GetOffsetsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:topic, 1, type: :string)
  field(:partition, 2, type: :int32)
end

defmodule Kafkaesque.GetOffsetsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:earliest, 1, type: :int64)
  field(:latest, 2, type: :int64)
end

defmodule Kafkaesque.CreateTopicRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:name, 1, type: :string)
  field(:partitions, 2, type: :int32)
  field(:batch_size, 3, type: :int32, json_name: "batchSize")
  field(:batch_timeout_ms, 4, type: :int32, json_name: "batchTimeoutMs")
  field(:min_demand, 5, type: :int32, json_name: "minDemand")
  field(:max_demand, 6, type: :int32, json_name: "maxDemand")
end

defmodule Kafkaesque.ListTopicsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3
end

defmodule Kafkaesque.ListTopicsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field(:topics, 1, repeated: true, type: Kafkaesque.Topic)
end

defmodule Kafkaesque.Kafkaesque.Service do
  @moduledoc false

  use GRPC.Service, name: "kafkaesque.Kafkaesque", protoc_gen_elixir_version: "0.15.0"

  rpc(:CreateTopic, Kafkaesque.CreateTopicRequest, Kafkaesque.Topic)

  rpc(:ListTopics, Kafkaesque.ListTopicsRequest, Kafkaesque.ListTopicsResponse)

  rpc(:Produce, Kafkaesque.ProduceRequest, Kafkaesque.ProduceResponse)

  rpc(:Consume, Kafkaesque.ConsumeRequest, stream(Kafkaesque.FetchedBatch))

  rpc(:CommitOffsets, Kafkaesque.CommitOffsetsRequest, Kafkaesque.CommitOffsetsResponse)

  rpc(:GetOffsets, Kafkaesque.GetOffsetsRequest, Kafkaesque.GetOffsetsResponse)
end

defmodule Kafkaesque.Kafkaesque.Stub do
  @moduledoc false

  use GRPC.Stub, service: Kafkaesque.Kafkaesque.Service
end
