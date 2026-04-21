# Multi-threaded YCSB Comparison (WAL Disabled)

- Config: `--compaction leveled --record-count 100000 --operation-count 100000 --seed 42 --warmup-ops 10000`
- WAL: disabled (did not pass `--enable-wal`)
- Matrix: workloads `a,b,c,e` x threads `1,2,4,8` x implementations `async,baseline`
- Async: shared tokio runtime (tokio::spawn) — Send-compatible futures

## Workload A

| Threads | Impl | Load ops/s | Run ops/s | p50 us | p95 us | p99 us | Run scaling vs t=1 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | async | 1,897,401 | 625,665 | 0.75 | 3.75 | 5.17 | 1.00x |
| 1 | baseline | 1,344,780 | 480,635 | 2.00 | 4.33 | 5.67 | 1.00x |
| 2 | async | 834,699 | 913,167 | 1.71 | 4.08 | 5.50 | 1.46x |
| 2 | baseline | 1,877,113 | 842,288 | 2.21 | 4.71 | 6.00 | 1.75x |
| 4 | async | 882,745 | 967,448 | 3.08 | 7.79 | 13.04 | 1.55x |
| 4 | baseline | 1,082,715 | 1,305,617 | 2.83 | 5.50 | 10.33 | 2.72x |
| 8 | async | 920,270 | 904,393 | 5.67 | 18.29 | 33.12 | 1.45x |
| 8 | baseline | 1,119,226 | 1,102,102 | 3.75 | 25.67 | 62.75 | 2.29x |

| Threads | Run throughput delta (async vs baseline) | p95 latency delta (async vs baseline) | p99 latency delta (async vs baseline) |
|---:|---:|---:|---:|
| 1 | +30.17% | -13.39% | -8.82% |
| 2 | +8.42% | -13.38% | -8.33% |
| 4 | -25.90% | +41.64% | +26.23% |
| 8 | -17.94% | -28.75% | -47.22% |

## Workload B (95% read, 5% write)

| Threads | Impl | Load ops/s | Run ops/s | p50 us | p95 us | p99 us | Run scaling vs t=1 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | async | 1,819,362 | 693,319 | 1.29 | 1.88 | 2.29 | 1.00x |
| 1 | baseline | 1,872,098 | 460,622 | 2.04 | 2.58 | 3.75 | 1.00x |
| 2 | async | 1,011,183 | 1,138,147 | 1.54 | 2.25 | 2.79 | 1.64x |
| 2 | baseline | 1,506,735 | 756,750 | 2.54 | 3.17 | 4.42 | 1.64x |
| 4 | async | 888,072 | 1,921,674 | 1.75 | 2.79 | 6.54 | 2.77x |
| 4 | baseline | 999,912 | 1,300,356 | 2.75 | 3.88 | 9.21 | 2.82x |
| 8 | async | 847,449 | 1,378,717 | 2.79 | 16.54 | 35.58 | 1.99x |
| 8 | baseline | 1,053,230 | 1,179,574 | 4.50 | 11.50 | 35.92 | 2.56x |

| Threads | Run throughput delta (async vs baseline) | p95 latency delta (async vs baseline) | p99 latency delta (async vs baseline) |
|---:|---:|---:|---:|
| 1 | +50.54% | -27.13% | -38.93% |
| 2 | +50.38% | -29.02% | -36.88% |
| 4 | +47.78% | -28.09% | -29.00% |
| 8 | +16.89% | +43.83% | -0.94% |

## Workload C

| Threads | Impl | Load ops/s | Run ops/s | p50 us | p95 us | p99 us | Run scaling vs t=1 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | async | 1,779,630 | 849,700 | 1.04 | 1.54 | 2.00 | 1.00x |
| 1 | baseline | 1,778,498 | 474,711 | 1.92 | 2.38 | 3.50 | 1.00x |
| 2 | async | 836,681 | 1,296,645 | 1.38 | 2.00 | 2.54 | 1.53x |
| 2 | baseline | 1,536,900 | 762,194 | 2.42 | 3.04 | 4.12 | 1.61x |
| 4 | async | 880,902 | 1,750,440 | 1.71 | 3.83 | 7.29 | 2.06x |
| 4 | baseline | 1,097,087 | 1,297,356 | 2.67 | 3.92 | 9.12 | 2.73x |
| 8 | async | 818,885 | 1,414,918 | 2.92 | 16.29 | 33.58 | 1.67x |
| 8 | baseline | 1,097,021 | 1,149,336 | 5.12 | 12.83 | 30.96 | 2.42x |

| Threads | Run throughput delta (async vs baseline) | p95 latency delta (async vs baseline) | p99 latency delta (async vs baseline) |
|---:|---:|---:|---:|
| 1 | +78.99% | -35.29% | -42.86% |
| 2 | +70.12% | -34.21% | -38.35% |
| 4 | +34.92% | -2.30% | -20.07% |
| 8 | +23.11% | +26.97% | +8.46% |

## Workload E

| Threads | Impl | Load ops/s | Run ops/s | p50 us | p95 us | p99 us | Run scaling vs t=1 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | async | 1,683,165 | 163,664 | 6.04 | 7.21 | 8.79 | 1.00x |
| 1 | baseline | 1,890,151 | 163,199 | 6.17 | 7.08 | 9.00 | 1.00x |
| 2 | async | 891,850 | 259,707 | 6.88 | 8.58 | 12.62 | 1.59x |
| 2 | baseline | 1,608,096 | 285,955 | 7.04 | 8.29 | 10.25 | 1.75x |
| 4 | async | 928,623 | 469,397 | 7.46 | 11.33 | 28.12 | 2.87x |
| 4 | baseline | 1,146,355 | 530,422 | 7.29 | 9.17 | 11.75 | 3.25x |
| 8 | async | 862,045 | 580,822 | 8.96 | 26.12 | 56.04 | 3.55x |
| 8 | baseline | 1,047,438 | 500,612 | 9.96 | 32.21 | 84.92 | 3.07x |

| Threads | Run throughput delta (async vs baseline) | p95 latency delta (async vs baseline) | p99 latency delta (async vs baseline) |
|---:|---:|---:|---:|
| 1 | +0.28% | +1.84% | -2.33% |
| 2 | -9.18% | +3.50% | +23.12% |
| 4 | -11.50% | +23.56% | +139.32% |
| 8 | +16.02% | -18.91% | -34.01% |

## Highlights

- Workload A: async peaks at t=4 (967,448 ops/s), baseline peaks at t=4 (1,305,617 ops/s).
- Workload C: async peaks at t=4 (1,750,440 ops/s), baseline peaks at t=4 (1,297,356 ops/s).
- Workload E: async peaks at t=8 (580,822 ops/s), baseline peaks at t=4 (530,422 ops/s).
- Positive throughput delta means async is faster; negative latency delta means async has lower latency.
- Results are single-run measurements; repeat runs recommended for confidence intervals.
