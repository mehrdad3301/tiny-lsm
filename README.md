# Tiny-LSM: Async LSM-Tree Storage Engine

[![CI (main)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml/badge.svg)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml)

TinyLSM is a key-value storage engine built on top of [mini-lsm](https://skyzh.github.io/mini-lsm) by Alex Chi Z, fully rewritten with async I/O using Tokio. Includes day-by-day solution checkpoints for the first three weeks and YCSB benchmarks comparing the async implementation against the original sync MVCC baseline.

## What This Project Does

- **Async engine** (`mini-lsm-starter/`) — complete LSM-tree with `async fn` on all I/O paths (WAL, SST reads, compaction, flush). Uses a shared tokio multi-threaded runtime.
- **SSTable prefetching** — adaptive block-level readahead (1–4 blocks) with a separate per-iterator prefetch buffer. Overlaps I/O for upcoming blocks with CPU work during sequential scans.
- **LZ4 block compression** — per-block compression reducing SST I/O transfer. Decompression cost is hidden behind async I/O on read-heavy workloads.
- **Day-by-day solutions** — check the commit history for day-by-day solution of the first three weeks

## YCSB Benchmark Results

Benchmarks run with `--compaction leveled --record-count 100000 --operation-count 100000 --seed 42 --warmup-ops 10000`, WAL disabled. The async engine includes both SSTable prefetching and LZ4 block compression.

### Single-Threaded (Workloads A–F)

| Workload | Async run ops/s | Baseline run ops/s | Delta |
|---|---:|---:|---:|
| A (50/50 r/w) | 789,981 | 558,668 | **+41.4%** |
| B (95/5 r/w) | 809,046 | 482,350 | **+67.7%** |
| C (100% read) | 955,995 | 503,948 | **+89.7%** |
| D (read latest) | 838,946 | 492,701 | **+70.3%** |
| E (short scans) | 175,456 | 165,751 | **+5.9%** |
| F (read-modify-write) | 472,229 | 302,117 | **+56.3%** |

### Multi-Threaded (Up to Saturation)

Both implementations saturate at 4 threads; 8-thread runs show lower throughput due to lock contention and I/O interference.

**Workload B (95% read, 5% write):**

| Threads | Async ops/s | Baseline ops/s | Throughput delta | p99 latency delta |
|---:|---:|---:|---:|---:|
| 1 | 775,466 | 460,858 | +68.3% | -45.2% |
| 2 | 1,225,064 | 765,137 | +60.1% | -43.4% |
| 4 | 1,815,109 | 1,375,782 | +31.9% | -17.0% |

**Workload C (100% read):**

| Threads | Async ops/s | Baseline ops/s | Throughput delta | p99 latency delta |
|---:|---:|---:|---:|---:|
| 1 | 967,624 | 500,890 | +93.2% | -52.0% |
| 2 | 1,462,232 | 785,834 | +86.1% | -45.2% |
| 4 | 2,323,526 | 1,478,923 | +57.1% | -2.8% |

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
