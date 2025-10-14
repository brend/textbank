//! # TextBank Benchmark Tool
//!
//! A comprehensive load testing and performance benchmarking tool for the TextBank gRPC service.
//!
//! This tool measures the performance characteristics of TextBank operations by simulating
//! realistic workloads with configurable parameters including:
//! - Concurrent client connections
//! - Operation volume and payload sizes
//! - Think time between operations
//! - Warmup periods for stable measurements
//!
//! ## Usage
//!
//! ```bash
//! cargo run --bin bench -- --target http://127.0.0.1:50051 --ops 100000 --concurrency 64
//! ```
//!
//! ## Metrics
//!
//! The benchmark reports detailed latency percentiles and throughput metrics,
//! with separate tracking for warmup and measured phases to ensure accuracy.

use clap::Parser;
use hdrhistogram::Histogram;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tokio::time::sleep;
use tonic::transport::Channel;

/// Alphabet used for generating random text payloads.
/// Includes common ASCII characters to simulate realistic text content.
const ALPH: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-.,:/";

/// Generated protobuf definitions for the TextBank service.
pub mod pb {
    tonic::include_proto!("textbank");
}
use pb::text_bank_client::TextBankClient;
use pb::{GetRequest, InternRequest};

/// Command-line arguments for the TextBank benchmark tool.
///
/// This struct defines all configurable parameters for the benchmark,
/// allowing fine-tuning of load testing scenarios.
#[derive(Parser, Debug)]
#[command(
    name = "textbank-bench",
    about = "Load benchmark for TextBank gRPC service",
    long_about = "A comprehensive performance testing tool that measures TextBank service \
                  latency and throughput under various load conditions. Each operation \
                  consists of an Intern (insert) followed by a Get (retrieve) to simulate \
                  realistic usage patterns."
)]
struct Args {
    /// Server address in the format http://host:port
    ///
    /// The target TextBank service endpoint to benchmark against.
    /// Must include the protocol (http:// or https://).
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    target: String,

    /// Total number of operations to execute during the measured phase
    ///
    /// Each operation consists of one Intern request followed by one Get request.
    /// The total number of gRPC calls will be 2x this value.
    #[arg(long, default_value_t = 100_000)]
    ops: usize,

    /// Number of concurrent worker tasks
    ///
    /// Controls the level of parallelism in the benchmark. Higher values
    /// increase load but may also increase contention and latency.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,

    /// Size of random text payload in bytes
    ///
    /// Determines the size of the random text content generated for each
    /// Intern operation. Larger payloads test serialization and network overhead.
    #[arg(long, default_value_t = 24)]
    payload_bytes: usize,

    /// Language code to use for all text entries
    ///
    /// The ISO language code used for all text storage operations.
    /// Affects indexing and retrieval performance characteristics.
    #[arg(long, default_value = "en")]
    lang: String,

    /// Number of warmup operations to execute before measurement begins
    ///
    /// Warmup operations allow the service to reach steady state by:
    /// - Warming up JIT compilation and caches
    /// - Establishing connection pools
    /// - Stabilizing memory allocation patterns
    ///
    /// Warmup metrics are tracked separately but not included in final results.
    #[arg(long, default_value_t = 10_000)]
    warmup_ops: usize,

    /// Optional think time between operations in microseconds
    ///
    /// Introduces a delay between consecutive operations to simulate
    /// realistic client behavior patterns. Set to 0 for maximum throughput testing.
    #[arg(long, default_value_t = 0)]
    pause_us: u64,
}

/// Creates a new gRPC client connection to the specified target.
///
/// # Arguments
/// * `target` - The server address to connect to
///
/// # Returns
/// * `TextBankClient<Channel>` - Connected gRPC client
///
/// # Panics
/// Panics if the connection cannot be established, as this is a fatal error
/// for the benchmark.
async fn mk_client(target: &str) -> TextBankClient<Channel> {
    TextBankClient::connect(target.to_string())
        .await
        .expect("Failed to connect to target server")
}

/// Main benchmark execution function.
///
/// Coordinates the entire benchmarking process including:
/// 1. Client connection establishment
/// 2. Warmup phase execution
/// 3. Measured phase execution with precise timing
/// 4. Results collection and reporting
///
/// The benchmark uses a barrier-based synchronization approach to ensure
/// all workers start the measured phase simultaneously for accurate results.
#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Print configuration summary
    println!("TextBank Benchmark Configuration:");
    println!("Target: {}", args.target);
    println!(
        "Concurrency: {} workers | Operations: {} | Payload: {}B | Warmup: {}",
        args.concurrency, args.ops, args.payload_bytes, args.warmup_ops
    );

    if args.pause_us > 0 {
        println!("Think time: {}μs between operations", args.pause_us);
    }
    println!();

    // Initialize shared histogram for latency measurements
    // Using bounds of 1ns to 60s with 3 significant figures for precision
    let hist = Arc::new(tokio::sync::Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("Failed to create measurement histogram"),
    ));

    let warm_hist = Arc::new(tokio::sync::Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("Failed to create warmup histogram"),
    ));

    // Pre-create clients to avoid connection overhead during benchmark
    // Each worker gets a dedicated client to eliminate connection contention
    let mut clients = Vec::with_capacity(args.concurrency);
    println!("Establishing {} client connections...", args.concurrency);

    for i in 0..args.concurrency {
        clients.push(mk_client(&args.target).await);
        if (i + 1) % 10 == 0 || i + 1 == args.concurrency {
            println!("Connected {}/{} clients", i + 1, args.concurrency);
        }
    }

    let clients = Arc::new(parking_lot::Mutex::new(clients));

    // Synchronization barriers for coordinated execution
    let barrier = Arc::new(Barrier::new(args.concurrency)); // End of warmup sync
    let start_gate = Arc::new(Barrier::new(args.concurrency)); // Start of measurement sync

    // Distribute work evenly across workers with remainder handling
    let warm_ops_per_worker = args.warmup_ops / args.concurrency.max(1);
    let ops_per_worker = args.ops / args.concurrency.max(1);

    let benchmark_start = Instant::now();
    let mut handles = Vec::new();

    println!("Starting warmup phase...");

    // Spawn worker tasks
    for worker_id in 0..args.concurrency {
        let clients = clients.clone();
        let barrier = barrier.clone();
        let start_gate = start_gate.clone();
        let hist = hist.clone();
        let warm_hist = warm_hist.clone();
        let lang = args.lang.clone();
        let payload_bytes = args.payload_bytes;
        let pause = args.pause_us;

        // Handle remainder operations by assigning them to the first worker
        let warm_ops = if worker_id == 0 {
            warm_ops_per_worker + (args.warmup_ops % args.concurrency)
        } else {
            warm_ops_per_worker
        };

        let measured_ops = if worker_id == 0 {
            ops_per_worker + (args.ops % args.concurrency)
        } else {
            ops_per_worker
        };

        let handle = tokio::spawn(async move {
            // Each worker gets a dedicated client to avoid contention
            let mut client = {
                let mut guard = clients.lock();
                guard.pop().expect("Client pool exhausted")
            };

            // Deterministic random number generator seeded per worker
            // This ensures reproducible payloads while avoiding correlation between workers
            let mut rng = StdRng::seed_from_u64(worker_id as u64 + 0xC0FFEE);
            let mut payload = vec![0u8; payload_bytes];

            // === WARMUP PHASE ===
            // Execute warmup operations to stabilize performance
            for _ in 0..warm_ops {
                // Generate random payload
                for byte in &mut payload {
                    *byte = ALPH[rng.gen_range(0..ALPH.len())];
                }

                let operation_start = Instant::now();

                // Execute Intern operation
                let intern_result = client
                    .intern(InternRequest {
                        lang: lang.clone(),
                        text: payload.clone(),
                        text_id: 0, // Auto-allocate ID
                    })
                    .await
                    .expect("Intern operation failed");

                let text_id = intern_result.into_inner().text_id;

                // Execute Get operation
                let _get_result = client
                    .get(GetRequest {
                        text_id,
                        lang: lang.clone(),
                    })
                    .await
                    .expect("Get operation failed");

                // Record warmup latency
                let operation_duration = operation_start.elapsed();
                {
                    let mut hist = warm_hist.lock().await;
                    hist.record(operation_duration.as_nanos() as u64).ok();
                }

                // Apply think time if configured
                if pause > 0 {
                    sleep(Duration::from_micros(pause)).await;
                }
            }

            // Wait for all workers to complete warmup
            barrier.wait().await;

            // Wait for synchronized start of measurement phase
            start_gate.wait().await;

            // === MEASUREMENT PHASE ===
            // Execute measured operations with precise timing
            for _ in 0..measured_ops {
                // Generate random payload
                for byte in &mut payload {
                    *byte = ALPH[rng.gen_range(0..ALPH.len())];
                }

                let operation_start = Instant::now();

                // Execute Intern operation
                let intern_result = client
                    .intern(InternRequest {
                        lang: lang.clone(),
                        text: payload.clone(),
                        text_id: 0, // Auto-allocate ID
                    })
                    .await
                    .expect("Intern operation failed during measurement");

                let text_id = intern_result.into_inner().text_id;

                // Execute Get operation
                let _get_result = client
                    .get(GetRequest {
                        text_id,
                        lang: lang.clone(),
                    })
                    .await
                    .expect("Get operation failed during measurement");

                // Record measurement latency
                let operation_duration = operation_start.elapsed();
                {
                    let mut hist = hist.lock().await;
                    hist.record(operation_duration.as_nanos() as u64).ok();
                }

                // Apply think time if configured
                if pause > 0 {
                    sleep(Duration::from_micros(pause)).await;
                }
            }

            // Return client to pool (not strictly necessary but good practice)
            {
                let mut guard = clients.lock();
                guard.push(client);
            }
        });

        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        handle.await.expect("Worker task panicked");
    }

    let total_elapsed = benchmark_start.elapsed();

    // Extract final histograms for analysis
    let final_hist = hist.lock().await.clone();
    let warmup_hist = warm_hist.lock().await.clone();

    // Calculate performance metrics
    let total_ops = args.ops as u64;
    let throughput_ops_per_sec = (total_ops as f64) / total_elapsed.as_secs_f64();

    // Print comprehensive results
    println!("\n=== BENCHMARK RESULTS ===");
    println!("Total elapsed time: {:.3}s", total_elapsed.as_secs_f64());
    println!("Measured operations: {} (Intern+Get pairs)", total_ops);
    println!(
        "Throughput: {:.0} operations/second",
        throughput_ops_per_sec
    );

    println!("\n--- Latency Distribution (per Intern+Get pair) ---");
    println!(
        "Nanoseconds  -> p50: {:>12}  p95: {:>12}  p99: {:>12}  p99.9: {:>12}  max: {:>12}",
        final_hist.value_at_quantile(0.50),
        final_hist.value_at_quantile(0.95),
        final_hist.value_at_quantile(0.99),
        final_hist.value_at_quantile(0.999),
        final_hist.max()
    );

    println!(
        "Microseconds -> p50: {:>12.1}  p95: {:>12.1}  p99: {:>12.1}  p99.9: {:>12.1}  max: {:>12.1}",
        final_hist.value_at_quantile(0.50) as f64 / 1_000.0,
        final_hist.value_at_quantile(0.95) as f64 / 1_000.0,
        final_hist.value_at_quantile(0.99) as f64 / 1_000.0,
        final_hist.value_at_quantile(0.999) as f64 / 1_000.0,
        final_hist.max() as f64 / 1_000.0
    );

    println!(
        "Milliseconds -> p50: {:>12.3}  p95: {:>12.3}  p99: {:>12.3}  p99.9: {:>12.3}  max: {:>12.3}",
        final_hist.value_at_quantile(0.50) as f64 / 1_000_000.0,
        final_hist.value_at_quantile(0.95) as f64 / 1_000_000.0,
        final_hist.value_at_quantile(0.99) as f64 / 1_000_000.0,
        final_hist.value_at_quantile(0.999) as f64 / 1_000_000.0,
        final_hist.max() as f64 / 1_000_000.0
    );

    // Report warmup statistics separately
    if args.warmup_ops > 0 {
        println!("\n--- Warmup Statistics (not included in throughput) ---");
        println!(
            "Warmup latency (μs): p50={:.1}  p95={:.1}  p99={:.1}  operations={}",
            warmup_hist.value_at_quantile(0.50) as f64 / 1_000.0,
            warmup_hist.value_at_quantile(0.95) as f64 / 1_000.0,
            warmup_hist.value_at_quantile(0.99) as f64 / 1_000.0,
            args.warmup_ops
        );
    }

    println!("\nBenchmark completed successfully!");
}
