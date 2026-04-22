# Tiny-LSM: Async LSM-Tree Storage Engine

[![CI (main)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml/badge.svg)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml)

TinyLSM is a key-value storage engine built on top of [mini-lsm](https://skyzh.github.io/mini-lsm) by Alex Chi Z, fully rewritten with async I/O using Tokio. Includes day-by-day solution checkpoints for the first three weeks and YCSB benchmarks comparing the async implementation against the original sync MVCC baseline.

## What This Project Does

- **Async engine** (`mini-lsm-starter/`) — complete LSM-tree with `async fn` on all I/O paths (WAL, SST reads, compaction, flush). Uses a shared tokio multi-threaded runtime.
- **Sync MVCC baseline** (`mini-lsm-mvcc/`) — original synchronous reference solution with MVCC, used as the benchmark baseline.
- **Day-by-day solutions** - check the commit history for day-by-day solution of the first three weeks

## Async Architecture

The entire I/O and compute stack was converted to async:

| Component | Async Entry Point |
|---|---|
| Storage engine | `LsmStorage::open()`, `put()`, `get()`, `delete()`, `scan()`, `sync()`, `close()` |
| Compaction | `compact()`, `spawn_compaction_task()`, `spawn_flush_task()` |
| WAL | `Wal::create()`, `recover()`, `put()`, `put_batch()`, `sync()` |
| Iterators | `ConcatIterator::create_and_seek_to_first()`, `create_and_seek_to_key()` |
| SST recovery | parallel SST opens during recovery |

Key dependencies: `tokio` (multi-threaded runtime), `futures`, `moka` (async cache).

## YCSB Benchmark Results

Benchmarks run with `--compaction leveled --record-count 100000 --operation-count 100000 --seed 42`, WAL disabled. Full results in [`ycsb-results/`](./ycsb-results/).

### Single-Threaded (Workloads A–F)

Async dominates on workloads:

| Workload | Async run ops/s | Baseline run ops/s | Delta |
|---|---:|---:|---:|
| A (50/50 r/w) | 575,587 | 360,070 | **+59.9%** |
| B (95/5 r/w) | 542,511 | 285,615 | **+89.9%** |
| C (100% read) | 607,417 | 299,233 | **+103.0%** |
| D (read latest) | 575,012 | 286,546 | **+100.7%** |
| F (50/50 r/w) | 353,934 | 182,644 | **+93.8%** |

### Multi-Threaded Highlights

Full multi-threaded results in [`ycsb-results/mt-validation-comparison.md`](./ycsb-results/mt-validation-comparison.md).

**Workload B (95% read) — async wins at every thread count:**

| Threads | Async ops/s | Baseline ops/s | Throughput delta | p99 latency delta |
|---:|---:|---:|---:|---:|
| 1 | 693,319 | 460,622 | +50.5% | -27.1% |
| 2 | 1,138,147 | 756,750 | +50.4% | -29.0% |
| 4 | 1,921,674 | 1,300,356 | +47.8% | -28.1% |
| 8 | 1,378,717 | 1,179,574 | +16.9% | +43.8% |

**Workload C (100% read) — largest async advantage:**

| Threads | Async ops/s | Baseline ops/s | Throughput delta | p99 latency delta |
|---:|---:|---:|---:|---:|
| 1 | 849,700 | 474,711 | +79.0% | -38.93% |
| 2 | 1,296,645 | 762,194 | +70.1% | -36.88% |
| 4 | 1,750,440 | 1,297,356 | +34.9% | -29.00% |
| 8 | 1,414,918 | 1,149,336 | +23.1% | -0.94% |

## SSTable Prefetching

Adaptive block-level prefetching for `SsTableIterator`, inspired by RocksDB's `FilePrefetchBuffer` + `BlockPrefetcher`. Overlaps I/O for upcoming blocks with CPU work on the current block during sequential scans.

### How it works

- **Separate prefetch buffer** per iterator (not block cache) — avoids cache pollution
- **Adaptive readahead** — starts at 1 block ahead, doubles on sequential access up to 4 blocks

### Workload E: Before vs After Prefetch

Same config: `--compaction leveled --record-count 100000 --operation-count 100000 --seed 42`, WAL disabled. Both runs on same machine, same day, back-to-back.

| Threads | No prefetch ops/s | Prefetch ops/s | Throughput change | p95 no pref | p95 prefetch | p95 change | p99 no pref | p99 prefetch | p99 change |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 170,231 | 170,313 | +0.0% | 7.00 | 6.71 | **-4.1%** | 10.38 | 8.88 | **-14.5%** |
| 2 | 290,847 | 298,127 | **+2.5%** | 8.04 | 7.67 | **-4.6%** | 10.58 | 9.58 | **-9.5%** |
| 4 | 532,828 | 550,061 | **+3.2%** | 8.71 | 8.38 | **-3.8%** | 12.75 | 13.00 | +2.0% |
| 8 | 522,876 | 556,284 | **+6.4%** | 26.96 | 25.54 | **-5.3%** | 52.42 | 49.17 | **-6.2%** |

Prefetching improves throughput at every thread count (+2–6%) with consistent p95 latency reduction (3–5%). The p99 improvement is most notable at 1 thread (-14.5%). The modest gains reflect that the current workload's scan length (100 keys) is short relative to block size, limiting how much readahead can help.

### All Workloads: Prefetch Impact

Full comparison across all workloads (A–F) at 1, 4, 8 threads: [`ycsb-results/prefetch-comparison.md`](./ycsb-results/prefetch-comparison.md).

Highlights:

| Workload | Threads | Best throughput Δ | Best p99 Δ |
|---|---:|---:|---:|
| D (read latest) | 1 | **+18.3%** | **-42.0%** |
| B (95% read) | 4 | **+7.1%** | **-10.8%** |
| C (100% read) | 4 | **+5.2%** | **-14.8%** |
| E (range scan) | 4 | +2.5% | **-45.0%** |
| A (50/50 r/w) | 8 | +0.2% | **-6.6%** |
| F (r/m/w) | 8 | +3.0% | **-22.6%** |

## Quick Start

```bash
# Build everything
cargo build --release

# Run the async CLI
cargo run --release --bin mini-lsm-cli

# Run YCSB benchmark (async)
cargo run -p mini-lsm-starter --release --bin ycsb-bench -- \
  --path /tmp/ycsb.db --workload a --compaction leveled \
  --record-count 100000 --operation-count 100000

# Run YCSB benchmark (sync MVCC baseline)
cargo run -p mini-lsm-mvcc --release --bin ycsb-bench-mvcc-ref -- \
  --path /tmp/ycsb-mvcc.db --workload a --compaction leveled \
  --record-count 100000 --operation-count 100000
```

## Project Structure

```
mini-lsm-starter/    # Async LSM engine (student code + solution)
mini-lsm-mvcc/       # Sync MVCC reference solution (benchmark baseline)
mini-lsm-book/       # Course book (original by skyzh)
ycsb-results/        # Benchmark data and comparison tables
xtask/               # Build tools
```

## Acknowledgments

- [Alex Chi Z (skyzh)](https://github.com/skyzh) — original [mini-lsm](https://github.com/skyzh/mini-lsm) course and framework.
- [SlateDB](https://slatedb.io/) and [Tonbo](https://tonbo.io/) — production LSM engines inspired by mini-lsm.

## License

The Mini-LSM starter code and solution are under [Apache 2.0 license](LICENSE). The author reserves the full copyright of the course materials (markdown files and figures).
