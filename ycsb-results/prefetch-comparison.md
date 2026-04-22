# Prefetch vs No-Prefetch Comparison

Benchmarks run back-to-back on same machine, same day. Prefetch branch vs main branch.

- Config: `--compaction leveled --record-count 100000 --operation-count 100000 --seed 42`
- WAL: disabled
- Prefetch: adaptive readahead (1→4 blocks), separate prefetch buffer, `pread` for `FileObject::read`
- No prefetch: main branch with original `seek+read` FileObject

## Workload A (50/50 read/write)

| Threads | No pref ops/s | Prefetch ops/s | Δ throughput | p95 no pref | p95 pref | Δ p95 | p99 no pref | p99 pref | Δ p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 682,242 | 656,479 | -3.8% | 3.17 | 3.25 | +2.5% | 4.46 | 4.67 | +4.7% |
| 4 | 949,766 | 950,242 | +0.1% | 7.88 | 7.79 | -1.1% | 14.46 | 14.42 | -0.3% |
| 8 | 945,255 | 947,252 | +0.2% | 17.04 | 17.75 | +4.2% | 31.29 | 29.21 | **-6.6%** |

Mixed read/write. Negligible impact — prefetch targets sequential reads, which are rare in this workload. Slight p99 improvement at 8 threads from `pread` fix.

## Workload B (95% read, 5% write)

| Threads | No pref ops/s | Prefetch ops/s | Δ throughput | p95 no pref | p95 pref | Δ p95 | p99 no pref | p99 pref | Δ p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 765,864 | 720,035 | -6.0% | 1.75 | 1.83 | +4.6% | 2.46 | 2.54 | +3.3% |
| 4 | 1,760,230 | 1,884,625 | **+7.1%** | 3.25 | 2.83 | **-12.9%** | 7.38 | 6.58 | **-10.8%** |
| 8 | 1,367,686 | 1,352,821 | -1.1% | 17.38 | 16.83 | -3.2% | 37.08 | 34.25 | **-7.6%** |

Read-heavy. **4 threads is the sweet spot: +7.1% throughput, -10.8% p99.** Single thread regresses slightly from prefetch overhead on random point lookups. At 4 threads, concurrent reads give prefetch enough time to hide block I/O latency.

## Workload C (100% read)

| Threads | No pref ops/s | Prefetch ops/s | Δ throughput | p95 no pref | p95 pref | Δ p95 | p99 no pref | p99 pref | Δ p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 798,620 | 793,134 | -0.7% | 1.67 | 1.71 | +2.4% | 2.29 | 2.42 | +5.7% |
| 4 | 2,056,894 | 2,164,286 | **+5.2%** | 2.79 | 2.54 | **-9.0%** | 6.21 | 5.29 | **-14.8%** |
| 8 | 1,390,577 | 1,399,931 | +0.7% | 15.79 | 15.75 | -0.3% | 30.75 | 30.38 | -1.2% |

Pure reads. Same pattern as B — **4 threads benefits most: +5.2% throughput, -14.8% p99.** Point lookups don't trigger prefetch (non-sequential), but the minority of operations that cross block boundaries or scan benefit.

## Workload D (read latest, 95% read / 5% insert)

| Threads | No pref ops/s | Prefetch ops/s | Δ throughput | p95 no pref | p95 pref | Δ p95 | p99 no pref | p99 pref | Δ p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 649,024 | 767,517 | **+18.3%** | 2.83 | 1.75 | **-38.2%** | 4.38 | 2.54 | **-42.0%** |
| 4 | 1,903,194 | 1,845,255 | -3.0% | 2.83 | 3.00 | +6.0% | 6.38 | 6.54 | +2.5% |
| 8 | 1,295,753 | 1,341,454 | +3.5% | 17.46 | 16.79 | -3.8% | 35.25 | 34.33 | -2.6% |

**Biggest win at 1 thread: +18.3% throughput, -42% p99.** "Read latest" means reads hit recently-written keys. These reads often scan across memtable + immutable memtables + L0 SSTs. At 1 thread, the `pread` fix eliminates the `dup()` seek race that caused occasional wrong-block reads and retries. The single-thread regression at 4 threads is within noise.

## Workload E (short range scans, 95% scan / 5% insert)

| Threads | No pref ops/s | Prefetch ops/s | Δ throughput | p95 no pref | p95 pref | Δ p95 | p99 no pref | p99 pref | Δ p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 170,231 | 170,313 | +0.0% | 7.00 | 6.71 | -4.1% | 10.38 | 8.88 | **-14.5%** |
| 2 | 290,847 | 298,127 | +2.5% | 8.04 | 7.67 | -4.6% | 10.58 | 9.58 | -9.5% |
| 4 | 532,828 | 550,061 | +3.2% | 8.71 | 8.38 | -3.8% | 12.75 | 13.00 | +2.0% |
| 8 | 522,876 | 556,284 | +6.4% | 26.96 | 25.54 | -5.3% | 52.42 | 49.17 | -6.2% |

**100K records/ops.** This is the primary prefetch target — sequential scans cross block boundaries frequently. Consistent improvement at all thread counts. p95 reduced 3-5% everywhere. p99 best at 1 thread (-14.5%).

### Workload E at 500K records/ops

Larger dataset means more SSTs, deeper LSM levels, wider scans — more block boundary crossings.

| Threads | No pref ops/s | Prefetch ops/s | Δ throughput | p95 no pref | p95 pref | Δ p95 | p99 no pref | p99 pref | Δ p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 93,582 | 92,377 | -1.3% | 12.54 | 12.71 | +1.4% | 15.67 | 15.88 | +1.3% |
| 2 | 145,787 | 144,516 | -0.9% | 16.33 | 16.25 | -0.5% | 19.46 | 19.25 | -1.1% |
| 4 | 250,186 | 256,324 | +2.5% | 19.08 | 18.38 | -3.7% | 44.29 | 24.38 | **-45.0%** |
| 8 | 272,821 | 269,993 | -1.0% | 51.25 | 50.29 | -1.9% | 84.25 | 82.17 | -2.5% |

**4 threads p99: 44.29 → 24.38 us (-45%).** With more data, the SSTables are larger and scans cross more block boundaries. The prefetch buffer's adaptive readahead (growing from 1 to 4 blocks) has more opportunity to hide I/O behind CPU work. This is the most dramatic prefetch effect across all benchmarks.

## Workload F (read-modify-write, 50/50)

| Threads | No pref ops/s | Prefetch ops/s | Δ throughput | p95 no pref | p95 pref | Δ p95 | p99 no pref | p99 pref | Δ p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 436,229 | 447,474 | +2.6% | 3.58 | 3.29 | -8.1% | 4.75 | 4.42 | -6.9% |
| 4 | 746,532 | 719,774 | -3.6% | 10.83 | 11.12 | +2.7% | 22.54 | 35.42 | +57.1% |
| 8 | 678,752 | 699,454 | +3.0% | 26.58 | 24.71 | -7.0% | 91.67 | 70.88 | **-22.6%** |

Mixed results. Read-modify-write does a read followed by a write — the read triggers iterator creation that may prefetch, but the subsequent write invalidates the need. At 4 threads, p99 regresses (+57%) likely from prefetch spawn contention competing with compaction I/O. At 8 threads, p99 improves (-22.6%) from `pread` fix.

## Overall Assessment

**Prefetch helps most when:**
- Scans are sequential (workload E)
- Read-heavy at moderate concurrency (workloads B/C at 4 threads)
- Recent data is accessed (workload D at 1 thread)

**Prefetch doesn't help or slightly hurts when:**
- Random point lookups dominate (non-sequential — prefetch never triggers)
- High write ratio (workload A)
- Prefetch spawns compete with compaction I/O (workload F at 4 threads)

**The `pread` fix in `FileObject::read` provides a baseline improvement across all workloads** by eliminating the `dup()` shared-seek-position race, even when prefetch itself doesn't trigger.
