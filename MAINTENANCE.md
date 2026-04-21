# TextBank Maintenance Notes

This file captures behavior that should remain stable across refactors.

## Core invariants

- Write ordering:
  - `Store::intern_with_id` must append to WAL before mutating in-memory indexes.
  - If WAL append fails, in-memory state (`forward`, `reverse`, `next_id`) must not change.
  - Writes are serialized by `write_lock` to keep WAL and index updates ordered.

- Reverse index semantics:
  - Reverse index is `(lang, hash) -> Vec<RevEntry>` (candidate bucket).
  - Dedup cannot rely on hash only; it must compare full normalized text.
  - Update mode must remove stale reverse mapping for the previous `(id, lang, text)` value.

- ID allocator:
  - `next_id` must only advance after successful WAL append for insert mode.
  - Explicit update IDs must advance allocator if they are >= current `next_id`.

- WAL replay:
  - Replays valid JSON-lines records in order.
  - A truncated final line is treated as a crash tail and ignored.
  - Corrupted non-tail records should fail startup (do not silently skip).

- Text normalization and encoding:
  - Input bytes are converted with `String::from_utf8_lossy` then NFC-normalized.
  - Invalid UTF-8 is currently accepted with replacement characters and dedups deterministically.

- Bench metrics:
  - Throughput must be measured from measured-phase start only (not warmup).
  - `--ops` and `--concurrency` must be non-zero.

## Regression checklist

Run before/after meaningful storage or API changes:

```bash
cargo test -q
cargo clippy -q
cargo bench --bench store_bench
```

Key tests currently cover:

- dedup idempotency and update behavior
- WAL append failure non-mutation
- truncated-tail replay vs non-tail corruption handling
- collision-safe dedup candidate matching
- language-scoped dedup
- allocator behavior after explicit IDs
- gRPC validation/error mapping and default-language retrieval
- bench argument validation

## Benchmark CI

- Workflow: `.github/workflows/benchmarks.yml`
- Cadence: weekly on Monday at 03:00 UTC (plus manual `workflow_dispatch`)
- Method:
  - Runs `cargo bench --bench store_bench`
  - Compares against last saved Criterion baseline (`ci`)
  - Fails only when the mean confidence interval lower bound exceeds `15%` regression
- Baseline storage:
  - Persisted via GitHub Actions cache key prefix `criterion-baseline-<os>-store-bench-`
  - Updated after successful runs

## Known constraints

- Durability currently uses `flush()` (not explicit `fsync`).
- WAL has no compaction yet.
- `RevEntry.text_arc` is retained intentionally for collision-safe candidate matching.
