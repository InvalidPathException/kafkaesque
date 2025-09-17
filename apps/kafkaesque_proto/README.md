# KafkaesqueProto

Shared protobuf definitions for the Kafkaesque distributed log system.

## Purpose

This umbrella app contains the protobuf definitions that are shared between:
- `kafkaesque_server` - The gRPC server implementation
- `kafkaesque_client` - The gRPC client SDK

By centralizing the protobuf definitions in a single app, we ensure:
- Single source of truth for API contracts
- Automatic synchronization between client and server
- No code duplication or version drift

## Structure

```
apps/kafkaesque_proto/
├── lib/
│   └── kafkaesque.pb.ex       # Generated protobuf Elixir code
├── priv/
│   └── protos/
│       └── kafkaesque.proto   # Source protobuf definitions
└── mix.exs
```

## Usage

### Generating Protobuf Code

From the project root:
```bash
mix proto.gen
```

Or from this app directory with PATH set correctly:
```bash
export PATH=$PATH:~/.mix/escripts
mix proto.gen
```

### Using in Other Apps

Add to your `mix.exs` dependencies:
```elixir
{:kafkaesque_proto, in_umbrella: true}
```

Then use the generated modules:
```elixir
alias Kafkaesque.{ProduceRequest, ProduceResponse}
alias Kafkaesque.Kafkaesque.Stub  # For client
alias Kafkaesque.Kafkaesque.Service  # For server
```

## API Messages

The protobuf definitions include:
- **Topic Management**: CreateTopicRequest, ListTopicsRequest/Response
- **Producing**: ProduceRequest/Response with Acks enum
- **Consuming**: ConsumeRequest, FetchedBatch (streaming)
- **Offsets**: CommitOffsetsRequest/Response, GetOffsetsRequest/Response
- **Core Types**: Topic, Record, RecordHeader

## Development

When modifying the proto file:
1. Edit `priv/protos/kafkaesque.proto`
2. Run `mix proto.gen` to regenerate code
3. Recompile dependent apps with `mix compile --force`

## Prerequisites

Ensure you have:
- `protoc` (Protocol Buffers compiler) installed
- `protoc-gen-elixir` plugin installed:
  ```bash
  mix escript.install hex protobuf
  export PATH=$PATH:~/.mix/escripts
  ```