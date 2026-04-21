use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use textbank::{Store, WalDurability};

static NEXT_WAL_ID: AtomicU64 = AtomicU64::new(1);

fn make_wal_path(label: &str) -> PathBuf {
    let id = NEXT_WAL_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "textbank_criterion_{}_{}_{}.jsonl",
        label,
        std::process::id(),
        id
    ))
}

fn make_store(label: &str) -> Store {
    let wal_path = make_wal_path(label);
    let _ = std::fs::remove_file(&wal_path);
    Store::new(wal_path, WalDurability::None).expect("failed to create benchmark store")
}

fn bench_intern(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_intern");

    group.bench_function("insert_new_text", |b| {
        let store = make_store("insert_new_text");
        let mut n = 0_u64;

        b.iter(|| {
            let payload = format!("payload-{n}");
            n += 1;
            let result = store
                .intern_with_id("en", payload.as_bytes(), None)
                .expect("insert should succeed");
            black_box(result);
        });
    });

    group.bench_function("dedup_existing_text", |b| {
        let store = make_store("dedup_existing_text");
        store
            .intern_with_id("en", b"constant payload", None)
            .expect("seed insert should succeed");

        b.iter(|| {
            let result = store
                .intern_with_id("en", black_box(b"constant payload"), None)
                .expect("dedup insert should succeed");
            black_box(result);
        });
    });

    group.finish();
}

fn bench_get_text(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_get_text");
    let store = make_store("get_text");
    let (id, _) = store
        .intern_with_id("en", b"hello benchmark world", None)
        .expect("seed insert should succeed");

    group.bench_function("hit", |b| {
        b.iter(|| {
            let found = store.get_text(black_box(id), Some("en"));
            black_box(found);
        });
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            let missing = store.get_text(black_box(id + 1), Some("en"));
            black_box(missing);
        });
    });

    group.finish();
}

fn bench_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_search");
    let store = make_store("search");

    for i in 0..10_000 {
        let payload = if i % 10 == 0 {
            format!("error timeout while processing request {i}")
        } else {
            format!("ok response {i}")
        };
        store
            .intern_with_id("en", payload.as_bytes(), Some(i + 1))
            .expect("seed insert should succeed");
    }

    group.bench_function("pattern_hit", |b| {
        b.iter(|| {
            let hits = store
                .search(black_box("error"), Some("en"))
                .expect("search should succeed");
            black_box(hits.len());
        });
    });

    group.bench_function("pattern_miss", |b| {
        b.iter(|| {
            let hits = store
                .search(black_box("definitely-not-present"), Some("en"))
                .expect("search should succeed");
            black_box(hits.len());
        });
    });

    group.finish();
}

criterion_group!(store_benches, bench_intern, bench_get_text, bench_search);
criterion_main!(store_benches);
