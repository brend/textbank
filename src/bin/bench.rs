use clap::Parser;
use hdrhistogram::Histogram;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tokio::time::sleep;
use tonic::transport::Channel;

const ALPH: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-.,:/";

pub mod pb {
    tonic::include_proto!("textbank");
}
use pb::text_bank_client::TextBankClient;
use pb::{GetRequest, InternRequest};

#[derive(Parser, Debug)]
#[command(name = "textbank-bench", about = "Load benchmark for TextBank gRPC service")]
struct Args {
    /// Server address (http://host:port)
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    target: String,
    /// Total number of operations (per op = 1 Intern + 1 Get)
    #[arg(long, default_value_t = 100_000)]
    ops: usize,
    /// Number of concurrent workers
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// Size of random text payload (bytes)
    #[arg(long, default_value_t = 24)]
    payload_bytes: usize,
    /// Language code to use
    #[arg(long, default_value = "en")]
    lang: String,
    /// Warmup operations (not counted)
    #[arg(long, default_value_t = 10_000)]
    warmup_ops: usize,
    /// Optional think time between ops (microseconds)
    #[arg(long, default_value_t = 0)]
    pause_us: u64,
}

async fn mk_client(target: &str) -> TextBankClient<Channel> {
    TextBankClient::connect(target.to_string())
        .await
        .expect("connect")
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("Target: {}", args.target);
    println!("Concurrency: {} | Ops: {} | Payload {}B | Warmup {}", args.concurrency, args.ops, args.payload_bytes, args.warmup_ops);

    // Shared histogram for end-to-end (Intern + Get) latencies
    let hist = Arc::new(tokio::sync::Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(), // 1ns..60s, 3 sig figs
    ));
    let warm_hist = Arc::new(tokio::sync::Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
    ));

    // Pre-create clients (one per worker) to avoid channel contention.
    let mut clients = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        clients.push(mk_client(&args.target).await);
    }

    let clients = Arc::new(parking_lot::Mutex::new(clients));
    let barrier = Arc::new(Barrier::new(args.concurrency));
    let start_gate = Arc::new(Barrier::new(args.concurrency));

    // Split warmup across workers
    let warm_ops_per_worker = args.warmup_ops / args.concurrency.max(1);
    let ops_per_worker = args.ops / args.concurrency.max(1);

    let t0 = Instant::now();

    let mut handles = Vec::new();
    for wid in 0..args.concurrency {
        let clients = clients.clone();
        let barrier = barrier.clone();
        let start_gate = start_gate.clone();
        let hist = hist.clone();
        let warm_hist = warm_hist.clone();
        let lang = args.lang.clone();
        let payload_bytes = args.payload_bytes;
        let pause = args.pause_us;
        let warm_n = if wid == 0 {
            // Give remainder to the first worker
            warm_ops_per_worker + (args.warmup_ops % args.concurrency)
        } else { warm_ops_per_worker };
        let n = if wid == 0 {
            ops_per_worker + (args.ops % args.concurrency)
        } else { ops_per_worker };

        let h = tokio::spawn(async move {
            // Get a dedicated client
            let mut client = {
                let mut guard = clients.lock();
                guard.pop().unwrap()
            };

            // Random generator per worker
            let mut rng = StdRng::seed_from_u64(wid as u64 + 0xC0FFEE);

            // Build a random payload template
            let mut payload = vec![0u8; payload_bytes];

            // Warmup
            for _ in 0..warm_n {
                for b in &mut payload {
    *b = ALPH[rng.gen_range(0..ALPH.len())];
}
                let t1 = Instant::now();
                let id = client
                    .intern(InternRequest { lang: lang.clone(), text: payload.clone() })
                    .await
                    .unwrap()
                    .into_inner()
                    .text_id;

                let _ = client
                    .get(GetRequest { text_id: id, lang: lang.clone() })
                    .await
                    .unwrap()
                    .into_inner();

                let dt = t1.elapsed();
                {
                    let mut h = warm_hist.lock().await;
                    h.record(dt.as_nanos() as u64).ok();
                }
                if pause > 0 { sleep(Duration::from_micros(pause)).await; }
            }

            barrier.wait().await; // ensure warmup for all workers done
            start_gate.wait().await; // start measured section together

            // Measured ops
            for _ in 0..n {
                for b in &mut payload {
    *b = ALPH[rng.gen_range(0..ALPH.len())];
}
                let t1 = Instant::now();

                let id = client
                    .intern(InternRequest { lang: lang.clone(), text: payload.clone() })
                    .await
                    .unwrap()
                    .into_inner()
                    .text_id;

                let _ = client
                    .get(GetRequest { text_id: id, lang: lang.clone() })
                    .await
                    .unwrap()
                    .into_inner();

                let dt = t1.elapsed();
                {
                    let mut h = hist.lock().await;
                    h.record(dt.as_nanos() as u64).ok();
                }
                if pause > 0 { sleep(Duration::from_micros(pause)).await; }
            }

            // Put client back (not strictly needed)
            {
                let mut guard = clients.lock();
                guard.push(client);
            }
        });
        handles.push(h);
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = t0.elapsed();
    let hist = hist.lock().await.clone();
    let warm_hist = warm_hist.lock().await.clone();

    let total_ops = args.ops as u64;
    let total_pairs_per_sec = (total_ops as f64) / elapsed.as_secs_f64();

    println!("\n=== Results (measured) ===");
    println!("Elapsed: {:.3}s", elapsed.as_secs_f64());
    println!("Ops (Intern+Get pairs): {}", total_ops);
    println!("Throughput: {:.0} pairs/sec", total_pairs_per_sec);
    println!("Latency per pair (nanos): p50={}  p95={}  p99={}  max={}",
        hist.value_at_quantile(0.50),
        hist.value_at_quantile(0.95),
        hist.value_at_quantile(0.99),
        hist.max()
    );
    println!("Latency per pair (micros): p50={:.1}  p95={:.1}  p99={:.1}  max={:.1}",
        hist.value_at_quantile(0.50) as f64 / 1_000.0,
        hist.value_at_quantile(0.95) as f64 / 1_000.0,
        hist.value_at_quantile(0.99) as f64 / 1_000.0,
        hist.max() as f64 / 1_000.0
    );

    println!("\n(Warmup stats, ignored in throughput): p50={}ns  p95={}ns  p99={}ns",
        warm_hist.value_at_quantile(0.50),
        warm_hist.value_at_quantile(0.95),
        warm_hist.value_at_quantile(0.99)
    );
}