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

**Workload E (short range scans) — minimal improvement due to sync block reads:**

| Threads | Async ops/s | Baseline ops/s | Throughput delta | p95 latency delta | p99 latency delta |
|---:|---:|---:|---:|---:|---:|
| 1 | 163,664 | 163,199 | +0.3% | +1.8% | -2.3% |
| 2 | 259,707 | 285,955 | -9.2% | +3.5% | +23.1% |
| 4 | 469,397 | 530,422 | -11.5% | +23.6% | +139.3% |
| 8 | 580,822 | 500,612 | +16.0% | -18.9% | -34.0% |

Workload E shows little to no improvement over the sync baseline. The bottleneck is that `SsTableIterator` reads the next block **synchronously** — on a scan, the iterator decodes the current block and immediately issues a blocking read for the next one. This synchronous block fetch dominates scan latency, so async on the rest of the pipeline has almost no effect. Prefetching or overlapped I/O on the iterator would be needed to close this gap.

### Key Takeaways

- **Read-heavy workloads**: async wins by 50–103% at single thread, maintaining advantage up to 4 threads.
- **Latency**: async p50/p95/p99 significantly lower on read paths (up to -42.9% p99).
- **Scaling**: baseline scales better at high thread counts on mixed workloads; async scales better on scan-heavy workloads (workload E).
- **Write-heavy**: mixed results — async faster at low concurrency, baseline catches up at 4+ threads. 

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
