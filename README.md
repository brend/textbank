# TextBank

A high-performance, fast text storage service with deduplication and multilingual support. TextBank provides a gRPC API for storing and retrieving text content with automatic normalization and efficient storage.

## Overview

TextBank is a proof-of-concept text storage service designed for high throughput and low latency. It features:

- **Idempotent Text Storage**: Store text once, get the same ID every time
- **Multilingual Support**: Store the same content in multiple languages
- **Fast Lookups**: Hash-based indexing for O(1) retrieval
- **Persistence**: Write-ahead log (WAL) for durability
- **Unicode Normalization**: Automatic NFC normalization for consistent storage
- **High Concurrency**: Built with Tokio for async performance

## Architecture

TextBank uses a dual-index approach:

- **Forward Index**: `text_id → (language → text)`
- **Reverse Index**: `(language, hash) → candidates` for deduplication

Text is normalized using Unicode NFC and hashed with xxHash3 for fast lookups. The service maintains durability through a JSON-lines write-ahead log.

## Features

### Core Operations

- **Intern**: Store text and get a unique ID (idempotent)
- **Get**: Retrieve text by ID and language

### Performance Characteristics

- Sub-millisecond latency for most operations
- Handles high concurrent load with dashmap data structures
- Memory-efficient with Arc-based string sharing
- Optional fsync for stronger durability guarantees

## Getting Started

### Prerequisites

- Rust 1.70+
- Protocol Buffers compiler (for development)

### Building

```bash
# Clone the repository
git clone <repository-url>
cd textbank

# Build the project
cargo build --release
```

### Running the Server

```bash
# Start the server (listens on 0.0.0.0:50051)
cargo run

# Or run the release build
./target/release/textbank
```

The server will:
- Create a `./data/` directory for the WAL file
- Load existing data from `./data/textbank.wal` on startup
- Listen for gRPC connections on port 50051

## Usage

### gRPC API

The service exposes two main operations via gRPC:

#### Intern
Store text and get an ID (idempotent operation):

```protobuf
rpc Intern(InternRequest) returns (InternReply);

message InternRequest { 
  string lang = 1; 
  bytes text = 2; 
}

message InternReply { 
  uint64 text_id = 1; 
  bool existed = 2; 
}
```

#### Get
Retrieve text by ID and language:

```protobuf
rpc Get(GetRequest) returns (GetReply);

message GetRequest { 
  uint64 text_id = 1; 
  string lang = 2; 
}

message GetReply { 
  bool found = 1; 
  bytes text = 2; 
}
```

### Command Line Interface

TextBank includes a convenient shell script wrapper for interacting with the service:

```bash
# Make the script executable
chmod +x textbank.sh

# Store some text
./textbank.sh intern en "Hello, World!"
# Output: {"textId":"1","existed":false}

# Retrieve the text
./textbank.sh get 1 en
# Output: Hello, World!

# Interactive mode
./textbank.sh repl
```

#### CLI Examples

```bash
# Store text in multiple languages
./textbank.sh intern en "Hello"
./textbank.sh intern es "Hola" 
./textbank.sh intern fr "Bonjour"

# Retrieve by ID and language
./textbank.sh get 1 en    # "Hello"
./textbank.sh get 2 es    # "Hola"

# Get raw base64 or JSON response
./textbank.sh get 1 en b64   # Base64 encoded
./textbank.sh get 1 en json  # Full JSON response
```

### Environment Variables

Configure the CLI tool with these environment variables:

- `TEXTBANK_TARGET`: Server address (default: `127.0.0.1:50051`)
- `TEXTBANK_PROTO_DIR`: Proto file directory (default: `./proto`)
- `TEXTBANK_PROTO_FILE`: Proto filename (default: `text_bank.proto`)
- `GRPCURL_BIN`: grpcurl binary path (default: `grpcurl`)

## Benchmarking

TextBank includes a comprehensive benchmark tool:

```bash
# Run default benchmark
cargo run --bin bench

# Custom benchmark parameters
cargo run --bin bench -- \
  --ops 1000000 \
  --concurrency 128 \
  --payload-bytes 50 \
  --target http://127.0.0.1:50051
```

### Benchmark Options

- `--ops`: Total number of operations (default: 100,000)
- `--concurrency`: Number of concurrent workers (default: 64)
- `--payload-bytes`: Size of random text payload (default: 24)
- `--lang`: Language code to use (default: "en")
- `--warmup-ops`: Warmup operations (default: 10,000)
- `--pause-us`: Think time between ops in microseconds (default: 0)

## Dependencies

### Core Dependencies

- **tokio**: Async runtime and utilities
- **tonic**: gRPC framework for Rust
- **prost**: Protocol Buffers implementation
- **dashmap**: Concurrent hash map
- **unicode-normalization**: Unicode NFC normalization
- **xxhash-rust**: Fast hashing algorithm
- **parking_lot**: High-performance synchronization primitives

### Development Dependencies

- **clap**: Command-line argument parsing (benchmarks)
- **hdrhistogram**: High dynamic range histograms (benchmarks)
- **rand**: Random number generation (benchmarks)

## Performance

TextBank is designed for high-performance scenarios:

- **Throughput**: Capable of handling tens of thousands of operations per second
- **Latency**: Sub-millisecond response times for most operations
- **Memory**: Efficient memory usage with string deduplication
- **Concurrency**: Lock-free data structures where possible

## Persistence

The service uses a simple but effective persistence model:

- **Write-Ahead Log**: JSON-lines format for human readability
- **Crash Recovery**: Automatic replay of WAL on startup
- **Durability**: Optional fsync for stronger guarantees
- **File Format**: One JSON object per line in `./data/textbank.wal`

Example WAL entries:
```json
{"text_id":1,"lang":"en","text":"Hello, World!"}
{"text_id":2,"lang":"es","text":"Hola, Mundo!"}
```

## Development

### Building from Source

```bash
# Development build
cargo build

# Release build with optimizations
cargo build --release

# Run tests
cargo test

# Format code
cargo fmt

# Lint code
cargo clippy
```

### Protocol Buffers

The gRPC schema is defined in `proto/text_bank.proto`. Changes require rebuilding:

```bash
# Regenerate Rust code from proto
cargo build  # build.rs handles code generation
```

## License

[Add your license information here]

## Contributing

[Add contributing guidelines here]

## Support

[Add support information here]