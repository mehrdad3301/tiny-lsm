# YCSB Comparison (WAL Disabled)

- Date: 2026-04-20
- Config: `--compaction leveled --record-count 100000 --operation-count 100000 --seed 42`
- WAL: disabled on both (did not pass `--enable-wal`)
- Workloads: A-F

## Raw Metrics

| Workload | Impl | Load ops/s | Run ops/s | p50 us | p95 us | p99 us |
|---|---:|---:|---:|---:|---:|---:|
| A | async | 1547392 | 575587 | 0.79 | 3.58 | 4.38 |
| A | baseline | 1504732 | 360070 | 3.04 | 5.58 | 6.38 |
| B | async | 1611268 | 542511 | 1.62 | 2.58 | 2.96 |
| B | baseline | 1465089 | 285615 | 3.33 | 4.33 | 4.83 |
| C | async | 1619229 | 607417 | 1.42 | 2.29 | 2.71 |
| C | baseline | 1527632 | 299233 | 3.08 | 3.92 | 4.46 |
| D | async | 1601239 | 575012 | 1.54 | 2.42 | 2.88 |
| D | baseline | 1572128 | 286546 | 3.33 | 4.21 | 4.83 |
| E | async | 1569359 | 141194 | 6.96 | 8.50 | 11.21 |
| E | baseline | 1552510 | 141399 | 7.04 | 8.08 | 11.12 |
| F | async | 1513787 | 353934 | 2.46 | 4.17 | 5.12 |
| F | baseline | 1911830 | 182644 | 4.58 | 7.42 | 14.04 |

## Delta (async vs baseline)

| Workload | Load throughput | Run throughput | p50 latency | p95 latency | p99 latency |
|---|---:|---:|---:|---:|---:|
| A | +2.84% | +59.85% | -74.01% | -35.84% | -31.35% |
| B | +9.98% | +89.94% | -51.35% | -40.42% | -38.72% |
| C | +6.00% | +102.99% | -53.90% | -41.58% | -39.24% |
| D | +1.85% | +100.67% | -53.75% | -42.52% | -40.37% |
| E | +1.09% | -0.14% | -1.14% | +5.20% | +0.81% |
| F | -20.82% | +93.78% | -46.29% | -43.80% | -63.53% |

## Notes

- Positive throughput delta means async is faster.
- Negative latency delta means async has lower latency.
- Results are single-run measurements and may vary between runs.
