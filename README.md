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
cd textbank
cargo build --release
```

### Running the Server

```bash
# Start the server (listens on 127.0.0.1:50051)
cargo run

# Or run the release build directly
./target/release/textbank
```

The server will:
- Create `./data/` directory for persistence
- Load existing data from `./data/wal.jsonl` on startup
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
- **Cross-language** (`lang` empty):
  - Evaluates the regex against all translations for each `text_id`
  - Returns each matching `text_id` at most once
  - Returns English text when available; otherwise returns a matching translation
- **Pattern**: Uses Rust regex syntax, applied to normalized text

## Interactive REPL

TextBank includes an interactive Read-Eval-Print Loop (REPL) for easy testing and exploration:

```bash
# Start the interactive REPL
cargo run --bin repl

# Or build and run directly
cargo build --bin repl
./target/debug/repl
```

The REPL automatically starts the TextBank service in the background and provides a user-friendly command-line interface.

### REPL Commands

```
Commands:
  help                           - Show help message
  quit, exit                     - Exit the REPL
  intern <lang> <text>           - Add text in specified language
  intern <id> <lang> <text>      - Add/update text with specific ID
  get <id> [lang]                - Get text by ID, optionally in specific language
  getall <id>                    - Get all translations for an ID
  search <pattern> [lang]        - Search texts using regex pattern (case-insensitive by default)
  stats                          - Show service statistics
```

### REPL Examples

```bash
textbank> intern en "Hello world"
✓ Text added with ID: 1

textbank> intern fr "Bonjour le monde"
✓ Text added with ID: 2

textbank> intern 1 fr "Bonjour le monde"
✓ Text added with ID: 1

textbank> get 1
ID 1: "Hello world"

textbank> get 1 fr
ID 1: "Bonjour le monde"

textbank> getall 1
✓ Found 2 translations for ID 1:
  en: "Hello world"
  fr: "Bonjour le monde"

textbank> search "hello.*world"
✓ Found 1 match:
  ID 1: "Hello world"

textbank> search "bonjour" fr
✓ Found 1 match:
  ID 1: "Bonjour le monde"

textbank> stats
TextBank Service Statistics:
  Service: Connected ✓
  Protocol: gRPC over HTTP/2
  Address: 127.0.0.1:50051
  Status: Service responding ✓

textbank> quit
Goodbye!
```

### REPL Features

- **Auto-start Service**: Automatically starts the TextBank gRPC service
- **Case-insensitive Search**: Search is case-insensitive by default
- **Quote-aware Parsing**: Supports shell-style quoting/escaping for arguments (e.g. `"Hello world"`, `\"`)
- **Readable Output**: Clear success/error indicators (✓/✗)
- **Command History**: Uses rustyline for command history and editing
- **Error Handling**: Graceful error messages with helpful hints, including parse errors for malformed input

### Advanced Search Patterns

The REPL supports full Rust regex syntax:

```bash
# Case-insensitive search (default)
textbank> search "hello.*world"

# Force case-sensitive search
textbank> search "(?-i)Hello.*World"

# Word boundaries
textbank> search "\bhello\b"

# Unicode patterns
textbank> search "café|coffee"

# Anchored patterns
textbank> search "^Hello"     # Starts with
textbank> search "world$"     # Ends with
```

## Command Line Interface

TextBank also includes a shell script wrapper for scripting and automation:

```bash
# Make executable
chmod +x textbank.sh

# Store text
./textbank.sh intern en "Hello, World!"
# Output: {"textId":"1","existed":false}

# Retrieve text  
./textbank.sh get 1 en
# Output: Hello, World!

# Start interactive script mode (intern/get/help/quit)
./textbank.sh repl
```

### CLI Examples

```bash
./textbank.sh intern en "Hello"
./textbank.sh get 1 en
./textbank.sh get 1 en b64
./textbank.sh get 1 en json
```

`textbank.sh` currently wraps only `Intern` and `Get`. Use the REPL (`cargo run --bin repl`) or a gRPC client for `GetAll` and `Search`.

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
- `--ops`: Total measured operations (default: 100,000, must be > 0)
- `--concurrency`: Concurrent workers (default: 64, must be > 0)
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

### Microbenchmarks (Criterion)

For in-process performance tracking of core `Store` operations (`intern_with_id`, `get_text`, `search`):

```bash
cargo bench --bench store_bench
```

This complements the gRPC load benchmark by isolating hot-path behavior inside the Rust storage layer.

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
- **Location**: `./data/wal.jsonl`
- **Durability**: Optional `flush()` after each write (configurable)
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
2. Ignores a truncated final WAL line (for crash-tolerant recovery)
3. Sets the next ID counter to `max_id + 1`
4. Resumes normal operation

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
- **rustyline**: Interactive REPL line editing and history
- **shell-words**: Shell-style REPL command parsing

## Text Processing

### Unicode Normalization

All text content is automatically normalized using Unicode NFC (Canonical Composition) to ensure:
- Consistent storage regardless of input encoding
- Proper deduplication of visually identical text
- Reliable search and comparison operations

### Hashing and Deduplication

TextBank uses XXH3-64 hashing of normalized text with language-scoped buckets:
- Bucket key: `(language, xxh3(normalized_text))`
- Fast hash computation for high throughput
- Collision resolution through full normalized-text comparison within each bucket

## Development

Maintenance notes for future work are in [`MAINTENANCE.md`](./MAINTENANCE.md).

### Building from Source

```bash
# Development build with debug symbols
cargo build

# Build all binaries (server, repl, benchmark)
cargo build --bins

# Optimized release build
cargo build --release

# Build specific binaries
cargo build --bin repl      # Interactive REPL
cargo build --bin bench     # Benchmark tool

# Run tests
cargo test --all-features --locked

# Code formatting
cargo fmt --all -- --check

# Linting
cargo clippy --all-targets --all-features -- -D warnings
```

### Continuous Integration

Primary quality checks run in `.github/workflows/ci.yml` on `push` and `pull_request`:
- formatting (`cargo fmt --all -- --check`)
- linting (`cargo clippy --all-targets --all-features -- -D warnings`)
- tests (`cargo test --all-features --locked`)

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
│   ├── lib.rs           # Library code (Store, service implementation)
│   ├── main.rs          # Main gRPC server binary
│   └── bin/
│       ├── repl.rs      # Interactive REPL
│       └── bench.rs     # Benchmark tool
├── proto/
│   └── text_bank.proto  # gRPC service definition  
├── data/                # WAL storage directory (created at runtime)
├── build.rs             # Protocol buffer code generation
├── textbank.sh          # CLI wrapper script
├── test_repl.sh         # REPL testing script
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

Dual-licensed as `MIT OR Apache-2.0` (see `Cargo.toml`).

## Contributing

Contributions are welcome. Please open an issue or pull request in your repository host.

## Support

Use your repository issue tracker for bugs and feature requests.
