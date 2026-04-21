# Multi-threaded YCSB Validation Matrix

This matrix verifies correctness (`threads=1` parity) and scaling (`threads=2,4,8`) for both implementations with WAL disabled.

## Common flags

- `--compaction leveled`
- `--record-count 100000`
- `--operation-count 100000`
- `--seed 42`
- `--warmup-ops 10000`
- no `--enable-wal`

## Workloads and thread levels

- Workloads: `a`, `c`, `e`
- Thread levels: `1`, `2`, `4`, `8`
- Total runs: `2 implementations x 3 workloads x 4 thread levels = 24`

## Starter (async) command template

```bash
cargo run -p mini-lsm-starter --release --bin ycsb-bench -- \
  --path "ycsb-results/mt-async-{workload}-t{threads}.db" \
  --workload "{workload}" \
  --threads "{threads}" \
  --compaction leveled \
  --record-count 100000 \
  --operation-count 100000 \
  --seed 42 \
  --warmup-ops 10000 \
  --report-per-thread
```

## Baseline (mvcc) command template

```bash
cargo run -p mini-lsm-mvcc --release --bin ycsb-bench-mvcc-ref -- \
  --path "ycsb-results/mt-baseline-{workload}-t{threads}.db" \
  --workload "{workload}" \
  --threads "{threads}" \
  --compaction leveled \
  --record-count 100000 \
  --operation-count 100000 \
  --seed 42 \
  --warmup-ops 10000 \
  --report-per-thread
```

## Capture and compare

For each run, capture:

- `Load throughput`
- `Run throughput`
- `p50`, `p95`, `p99`, `p99.9`
- per-thread ops/sec lines when `--report-per-thread` is enabled

Then compare:

- scaling ratio vs `threads=1` within each implementation
- async vs baseline at each `(workload, threads)` point
