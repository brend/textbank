# TextBank

A high-performance, multilingual text storage and retrieval service built with Rust and gRPC.

## Overview

TextBank provides efficient storage, retrieval, and search capabilities for multilingual text content with features including:

- **Text Normalization**: Automatic Unicode NFC normalization for consistent storage
- **Hash-based Deduplication**: Efficient storage with content-based deduplication
- **Language-aware Storage**: Store and retrieve text in multiple languages
- **Regex-based Search**: Search across languages using regular expressions
- **Write-ahead Logging**: Durability guarantees with crash recovery
- **High-performance Concurrency**: Built with lock-free data structures for maximum throughput

## Architecture

TextBank uses a dual-index approach for optimal performance:

- **Forward Index**: `text_id → (language → text_content)` - Fast retrieval by ID
- **Reverse Index**: `(language, content_hash) → [candidates]` - Efficient deduplication and existence checks

All text content is normalized using Unicode NFC and indexed using XXH3 hashing for fast lookups. The service maintains durability through a simple JSON-lines write-ahead log.

## Getting Started

### Prerequisites

- Rust 1.70+ 
- Protocol Buffers compiler (for development)

### Building

```bash
git clone <repository-url>
cd textbank
cargo build --release
```

### Running the Server

```bash
# Start the server (listens on 0.0.0.0:50051)
cargo run

# Or run the release build directly
./target/release/textbank
```

The server will:
- Create `./data/` directory for persistence
- Load existing data from `./data/textbank.wal` on startup
- Listen for gRPC connections on port 50051

## API Reference

TextBank exposes a comprehensive gRPC API with four main operations:

### 1. Intern - Store Text Content

Insert or update text translations with optional ID specification.

```protobuf
rpc Intern(InternRequest) returns (InternReply);
```

**Request:**
```protobuf
message InternRequest {
  bytes text = 1;        // UTF-8 encoded text content
  string lang = 2;       // Language code (required, e.g., "en", "fr", "de")
  uint64 text_id = 3;    // Optional: 0 = allocate/idempotent, >0 = update specific ID
}
```

**Response:**
```protobuf
message InternReply {
  uint64 text_id = 1;    // Unique text identifier
  bool existed = 2;      // true if content already existed or ID was updated
}
```

**Behavior:**
- **Insert Mode** (`text_id = 0`): Idempotent insert by content
  - Returns existing ID if (language, text) pair already exists
  - Allocates new ID for new content
- **Update Mode** (`text_id > 0`): Updates or inserts text for specific ID

### 2. Get - Retrieve Text Content

Retrieve text by ID with optional language filtering.

```protobuf
rpc Get(GetRequest) returns (GetReply);
```

**Request:**
```protobuf
message GetRequest {
  uint64 text_id = 1;    // Text identifier to retrieve
  string lang = 2;       // Optional: specific language, empty = preferred language
}
```

**Response:**
```protobuf
message GetReply {
  bool found = 1;        // Whether text was found
  bytes text = 2;        // UTF-8 encoded text content (if found)
}
```

**Language Selection:**
- **Specified language**: Returns text in that language if available
- **Empty language**: Returns preferred language (English first, then any available)

### 3. GetAll - Retrieve All Translations

Get all language translations for a specific text ID.

```protobuf
rpc GetAll(GetAllRequest) returns (GetAllReply);
```

**Request:**
```protobuf
message GetAllRequest {
  uint64 text_id = 1;    // Text identifier
}
```

**Response:**
```protobuf
message GetAllReply {
  uint64 text_id = 1;           // Echo of requested ID
  repeated Translation translations = 2;  // All available translations
}

message Translation {
  string lang = 1;       // Language code
  bytes text = 2;        // UTF-8 encoded text content
}
```

### 4. Search - Regex-based Text Search

Search text content using regular expressions with optional language filtering.

```protobuf
rpc Search(SearchRequest) returns (SearchReply);
```

**Request:**
```protobuf
message SearchRequest {
  string pattern = 1;    // Rust regex syntax pattern
  string lang = 2;       // Optional: specific language, empty = all languages
}
```

**Response:**
```protobuf
message SearchReply {
  repeated SearchHit hits = 1;  // Matching results
}

message SearchHit {
  uint64 text_id = 1;    // Text identifier of match
  bytes text = 2;        // Matching text content
}
```

**Search Behavior:**
- **Language-specific** (`lang` specified): Searches only within that language
- **Cross-language** (`lang` empty): Searches all languages, preferring English results
- **Pattern**: Uses Rust regex syntax, applied to normalized text

## Command Line Interface

TextBank includes a shell script wrapper for easy interaction:

```bash
# Make executable
chmod +x textbank.sh

# Store text
./textbank.sh intern en "Hello, World!"
# Output: {"textId":"1","existed":false}

# Retrieve text  
./textbank.sh get 1 en
# Output: Hello, World!

# Get all translations
./textbank.sh get-all 1
# Output: {"textId":"1","translations":[{"lang":"en","text":"SGVsbG8sIFdvcmxkIQ=="}]}

# Search text
./textbank.sh search "Hello.*"
# Output: {"hits":[{"textId":"1","text":"SGVsbG8sIFdvcmxkIQ=="}]}

# Interactive mode
./textbank.sh repl
```

### CLI Examples

```bash
# Multilingual content
./textbank.sh intern en "Hello"
./textbank.sh intern fr "Bonjour" 1  # Update ID 1 with French
./textbank.sh intern es "Hola" 1     # Add Spanish to ID 1

# Retrieve specific languages
./textbank.sh get 1 en    # "Hello"
./textbank.sh get 1 fr    # "Bonjour"
./textbank.sh get 1 es    # "Hola"

# Get all translations
./textbank.sh get-all 1   # Returns all languages for ID 1

# Search examples
./textbank.sh search "^H.*"          # Words starting with H
./textbank.sh search "ell" en        # "ell" in English only
./textbank.sh search "(?i)hello"     # Case-insensitive search
```

### Environment Variables

Configure the CLI tool:

- `TEXTBANK_TARGET`: Server address (default: `127.0.0.1:50051`)
- `TEXTBANK_PROTO_DIR`: Proto directory (default: `./proto`)
- `TEXTBANK_PROTO_FILE`: Proto filename (default: `text_bank.proto`)
- `GRPCURL_BIN`: grpcurl binary path (default: `grpcurl`)

## Performance Benchmarking

TextBank includes a comprehensive benchmarking tool:

```bash
# Default benchmark
cargo run --bin bench

# Custom configuration
cargo run --bin bench -- \
  --target http://127.0.0.1:50051 \
  --ops 1000000 \
  --concurrency 128 \
  --payload-bytes 64 \
  --warmup-ops 50000
```

### Benchmark Options

- `--target`: Server address (default: `http://127.0.0.1:50051`)
- `--ops`: Total measured operations (default: 100,000)
- `--concurrency`: Concurrent workers (default: 64)
- `--payload-bytes`: Random text size (default: 24)
- `--lang`: Language code (default: `en`)
- `--warmup-ops`: Warmup operations (default: 10,000)  
- `--pause-us`: Think time in microseconds (default: 0)

### Benchmark Metrics

The tool provides detailed performance metrics:

```
=== BENCHMARK RESULTS ===
Total elapsed time: 12.456s
Measured operations: 1000000 (Intern+Get pairs)
Throughput: 80288 operations/second

--- Latency Distribution (per Intern+Get pair) ---
Nanoseconds  -> p50:     12434  p95:     28901  p99:     45123  p99.9:     78456  max:    234567
Microseconds -> p50:      12.4  p95:      28.9  p99:      45.1  p99.9:      78.5  max:     234.6
Milliseconds -> p50:     0.012  p95:     0.029  p99:     0.045  p99.9:     0.078  max:     0.235
```

## Performance Characteristics

TextBank is optimized for high-throughput scenarios:

- **Throughput**: 50,000+ operations/second on modern hardware
- **Latency**: Sub-millisecond response times for most operations
- **Concurrency**: Lock-free data structures with DashMap
- **Memory**: Efficient with Arc-based string sharing and deduplication
- **Scalability**: Handles thousands of concurrent connections

## Persistence and Durability

### Write-Ahead Log (WAL)

TextBank uses a simple but effective persistence model:

- **Format**: JSON-lines for human readability and debugging
- **Location**: `./data/textbank.wal`
- **Durability**: Optional fsync after each write (configurable)
- **Recovery**: Automatic replay on startup

Example WAL entries:
```json
{"text_id":1,"lang":"en","text":"Hello, World!"}
{"text_id":1,"lang":"fr","text":"Bonjour, le monde!"}
{"text_id":2,"lang":"en","text":"Welcome"}
```

### Crash Recovery

On startup, TextBank:
1. Replays all WAL entries to rebuild indexes
2. Sets the next ID counter to `max_id + 1`
3. Resumes normal operation

## Dependencies

### Core Runtime
- **tokio**: Async runtime and utilities
- **tonic**: gRPC framework for Rust  
- **prost**: Protocol Buffers implementation
- **dashmap**: Lock-free concurrent hash map
- **unicode-normalization**: Unicode NFC normalization
- **xxhash-rust**: Fast XXH3 hashing algorithm
- **parking_lot**: High-performance synchronization primitives
- **serde**: Serialization framework
- **anyhow**: Error handling

### Development & Benchmarking
- **clap**: Command-line argument parsing
- **hdrhistogram**: High dynamic range histograms
- **rand**: Random number generation
- **regex**: Regular expression engine

## Text Processing

### Unicode Normalization

All text content is automatically normalized using Unicode NFC (Canonical Composition) to ensure:
- Consistent storage regardless of input encoding
- Proper deduplication of visually identical text
- Reliable search and comparison operations

### Hashing and Deduplication

TextBank uses XXH3-64 hashing with a language-specific salt:
- Hash key: `language + "\u{001F}" + normalized_text`
- Non-ASCII separator reduces collision probability
- Fast hash computation for high throughput
- Collision resolution through content comparison

## Development

### Building from Source

```bash
# Development build with debug symbols
cargo build

# Optimized release build
cargo build --release

# Run tests
cargo test

# Code formatting
cargo fmt

# Linting
cargo clippy
```

### Protocol Buffer Schema

The gRPC API is defined in `proto/text_bank.proto`. After changes:

```bash
# Regenerate Rust code (handled by build.rs)
cargo build
```

### Project Structure

```
textbank/
├── src/
│   ├── main.rs          # Main service implementation
│   └── bin/
│       └── bench.rs     # Benchmark tool
├── proto/
│   └── text_bank.proto  # gRPC service definition  
├── data/                # WAL storage directory (created at runtime)
├── build.rs             # Protocol buffer code generation
├── textbank.sh          # CLI wrapper script
└── Cargo.toml           # Dependencies and metadata
```

## Use Cases

TextBank is well-suited for:

- **Content Management Systems**: Multilingual content storage and retrieval
- **Translation Services**: Managing text in multiple languages
- **Search Applications**: Fast text search with regex support
- **Caching Layers**: High-performance text caching with persistence
- **Data Pipelines**: Text processing and normalization services

## Limitations

Current limitations and future enhancements:

- **Single Node**: No built-in clustering or replication
- **Memory Bound**: All data must fit in memory
- **Simple Persistence**: Basic WAL without compaction
- **No Authentication**: No built-in security mechanisms
- **Limited Query**: Only regex search, no full-text search

## License

[Add your license information here]

## Contributing

[Add contributing guidelines here]

## Support

For questions, issues, or contributions:
- [Add issue tracker URL]
- [Add discussion forum/chat]
- [Add documentation links]