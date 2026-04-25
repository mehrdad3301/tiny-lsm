// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use mini_lsm_starter::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
    TieredCompactionOptions,
};
use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
// use rand::distributions::Distribution;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone, ValueEnum)]
enum CompactionStrategy {
    Simple,
    Leveled,
    Tiered,
    None,
}

#[derive(Debug, Clone, ValueEnum)]
enum YcsbWorkload {
    A, // Update heavy: 50% read, 50% update
    B, // Read mostly: 95% read, 5% update
    C, // Read only: 100% read
    D, // Read latest: 95% read, 5% insert
    E, // Short ranges: 95% scan, 5% insert
    F, // Read-modify-write: 50% read, 50% read-modify-write
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "YCSB-style benchmark for Mini-LSM", long_about = None)]
struct Args {
    #[arg(long, default_value = "ycsb.db")]
    path: PathBuf,
    #[arg(long, default_value = "leveled")]
    compaction: CompactionStrategy,
    #[arg(long, default_value = "A")]
    workload: YcsbWorkload,
    #[arg(long, default_value = "100000")]
    record_count: u64,
    #[arg(long, default_value = "100000")]
    operation_count: u64,
    #[arg(long, default_value = "100")]
    scan_length: usize,
    #[arg(long, default_value = "100")]
    value_size: usize,
    #[arg(long)]
    enable_wal: bool,
    #[arg(long, default_value = "4096")]
    block_size: usize,
    #[arg(long, default_value = "2097152")]
    target_sst_size: usize,
    #[arg(long, default_value = "50")]
    num_memtable_limit: usize,
    #[arg(long, default_value = "42")]
    seed: u64,
    #[arg(long)]
    zipfian: bool,
    #[arg(long, default_value = "1")]
    threads: usize,
    #[arg(long, default_value = "0")]
    warmup_ops: u64,
    #[arg(long)]
    report_per_thread: bool,
}

fn make_key(i: u64) -> Vec<u8> {
    format!("user{:016}", i).into_bytes()
}

fn make_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn build_options(args: &Args) -> LsmStorageOptions {
    let compaction_options = match args.compaction {
        CompactionStrategy::Simple => CompactionOptions::Simple(SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 2,
            max_levels: 4,
        }),
        CompactionStrategy::Leveled => CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 10,
            level0_file_num_compaction_trigger: 2,
            max_levels: 4,
            base_level_size_mb: 128,
        }),
        CompactionStrategy::Tiered => CompactionOptions::Tiered(TieredCompactionOptions {
            num_tiers: 3,
            max_size_amplification_percent: 200,
            size_ratio: 1,
            min_merge_width: 2,
            max_merge_width: None,
        }),
        CompactionStrategy::None => CompactionOptions::NoCompaction,
    };

    LsmStorageOptions {
        block_size: args.block_size,
        target_sst_size: args.target_sst_size,
        num_memtable_limit: args.num_memtable_limit,
        compaction_options,
        enable_wal: args.enable_wal,
        serializable: false,
        group_commit: true,
        group_commit_timeout_ms: 10,
        group_commit_max_batch: 100,
    }
}

/// Zipfian distribution sampler for skewed key access.
struct Zipfian {
    n: u64,
    theta: f64,
    zetan: f64,
}

impl Zipfian {
    fn new(n: u64, theta: f64) -> Self {
        let zetan = (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        Self { n, theta, zetan }
    }

    fn sample<R: Rng>(&self, rng: &mut R) -> u64 {
        let u = rng.r#gen::<f64>();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        let mut low = 1u64;
        let mut high = self.n;
        while low < high {
            let mid = (low + high) / 2;
            let sum = (1..=mid).map(|i| 1.0 / (i as f64).powf(self.theta)).sum::<f64>();
            if sum < uz {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low - 1
    }
}

async fn load_phase(storage: &Arc<MiniLsm>, record_count: u64, value_size: usize, _rng: &mut StdRng) -> Result<Duration> {
    println!("[LOAD] Inserting {} records...", record_count);
    let start = Instant::now();
    for i in 0..record_count {
        let key = make_key(i);
        let value = make_value(value_size);
        storage.put(&key, &value).await?;
    }
    let elapsed = start.elapsed();
    println!("[LOAD] Done in {:?} ({:.0} ops/sec)", elapsed, record_count as f64 / elapsed.as_secs_f64());
    Ok(elapsed)
}

#[derive(Default)]
struct WorkerStats {
    ops: u64,
    elapsed: Duration,
    latencies: Vec<f64>,
}

fn split_work(total: u64, workers: usize, worker_id: usize) -> u64 {
    let workers_u64 = workers as u64;
    let base = total / workers_u64;
    let rem = total % workers_u64;
    base + u64::from((worker_id as u64) < rem)
}

fn normalize_threads(threads: usize) -> usize {
    threads.max(1)
}

async fn load_phase_parallel(storage: &Arc<MiniLsm>, record_count: u64, value_size: usize, threads: usize) -> Result<Duration> {
    println!("[LOAD] Inserting {} records with {} workers...", record_count, threads);
    let start = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for worker_id in 0..threads {
        let storage = Arc::clone(storage);
        handles.push(tokio::spawn(async move {
            let mut inserted = 0u64;
            for i in (worker_id as u64..record_count).step_by(threads) {
                let key = make_key(i);
                let value = make_value(value_size);
                storage.put(&key, &value).await?;
                inserted += 1;
            }
            Ok::<u64, anyhow::Error>(inserted)
        }));
    }

    let mut inserted_total = 0u64;
    for handle in handles {
        inserted_total += handle.await??;
    }
    let elapsed = start.elapsed();
    println!(
        "[LOAD] Done in {:?} ({:.0} ops/sec)",
        elapsed,
        inserted_total as f64 / elapsed.as_secs_f64()
    );
    Ok(elapsed)
}

async fn worker_run_loop(
    storage: Arc<MiniLsm>,
    args: Arc<Args>,
    worker_id: usize,
    op_count: u64,
    insert_counter: Arc<AtomicU64>,
    collect_latencies: bool,
) -> Result<WorkerStats> {
    let mut rng = StdRng::seed_from_u64(args.seed.wrapping_add(worker_id as u64));
    let zipf = if args.zipfian {
        Some(Zipfian::new(args.record_count, 0.99))
    } else {
        None
    };
    let mut latencies = if collect_latencies {
        Vec::with_capacity(op_count as usize)
    } else {
        Vec::new()
    };
    let start = Instant::now();

    for _ in 0..op_count {
        let key_idx = match &zipf {
            Some(z) => z.sample(&mut rng),
            None => rng.gen_range(0..args.record_count),
        };
        let key = make_key(key_idx);
        let op_start = Instant::now();
        match args.workload {
            YcsbWorkload::A => {
                if rng.gen_bool(0.5) {
                    storage.get(&key).await?;
                } else {
                    let value = make_value(args.value_size);
                    storage.put(&key, &value).await?;
                }
            }
            YcsbWorkload::B => {
                if rng.gen_bool(0.95) {
                    storage.get(&key).await?;
                } else {
                    let value = make_value(args.value_size);
                    storage.put(&key, &value).await?;
                }
            }
            YcsbWorkload::C => {
                storage.get(&key).await?;
            }
            YcsbWorkload::D => {
                if rng.gen_bool(0.95) {
                    let latest = insert_counter.load(Ordering::Relaxed);
                    let read_key = make_key(rng.gen_range(0..latest.max(1)));
                    storage.get(&read_key).await?;
                } else {
                    let insert_id = insert_counter.fetch_add(1, Ordering::Relaxed);
                    let insert_key = make_key(insert_id);
                    let value = make_value(args.value_size);
                    storage.put(&insert_key, &value).await?;
                }
            }
            YcsbWorkload::E => {
                if rng.gen_bool(0.95) {
                    let mut iter = storage.scan(Bound::Included(&key), Bound::Unbounded).await?;
                    for _ in 0..args.scan_length {
                        if !iter.is_valid() {
                            break;
                        }
                        iter.next()?;
                    }
                } else {
                    let insert_id = insert_counter.fetch_add(1, Ordering::Relaxed);
                    let insert_key = make_key(insert_id);
                    let value = make_value(args.value_size);
                    storage.put(&insert_key, &value).await?;
                }
            }
            YcsbWorkload::F => {
                if rng.gen_bool(0.5) {
                    storage.get(&key).await?;
                } else {
                    let _ = storage.get(&key).await?;
                    let value = make_value(args.value_size);
                    storage.put(&key, &value).await?;
                }
            }
        }
        if collect_latencies {
            latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
        }
    }

    Ok(WorkerStats {
        ops: op_count,
        elapsed: start.elapsed(),
        latencies,
    })
}

async fn run_workload_parallel(
    storage: &Arc<MiniLsm>,
    args: &Args,
    op_count: u64,
    collect_latencies: bool,
) -> Result<(Duration, Vec<f64>, Vec<WorkerStats>)> {
    let threads = normalize_threads(args.threads);
    let args = Arc::new(args.clone());
    let insert_counter = Arc::new(AtomicU64::new(args.record_count));

    let mut handles = Vec::with_capacity(threads);
    let start = Instant::now();
    for worker_id in 0..threads {
        let worker_ops = split_work(op_count, threads, worker_id);
        let storage = Arc::clone(storage);
        let args = Arc::clone(&args);
        let insert_counter = Arc::clone(&insert_counter);
        handles.push(tokio::spawn(async move {
            worker_run_loop(
                storage,
                args,
                worker_id,
                worker_ops,
                insert_counter,
                collect_latencies,
            )
            .await
        }));
    }

    let mut workers = Vec::with_capacity(threads);
    let mut latencies = Vec::new();
    for handle in handles {
        let worker = handle.await??;
        if collect_latencies {
            latencies.extend_from_slice(&worker.latencies);
        }
        workers.push(worker);
    }

    Ok((start.elapsed(), latencies, workers))
}

fn report_latencies(latencies: &mut [f64]) {
    if latencies.is_empty() {
        println!("\n[Latency] No operations recorded.");
        return;
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = latencies.len();
    let p50 = latencies[n * 50 / 100];
    let p95 = latencies[n * 95 / 100];
    let p99 = latencies[n * 99 / 100];
    let p999 = latencies[n * 999 / 1000];
    let avg = latencies.iter().sum::<f64>() / n as f64;
    let min = latencies[0];
    let max = latencies[n - 1];

    println!("\n[Latency] microseconds:");
    println!("  min   = {:.2}", min);
    println!("  avg   = {:.2}", avg);
    println!("  p50   = {:.2}", p50);
    println!("  p95   = {:.2}", p95);
    println!("  p99   = {:.2}", p99);
    println!("  p99.9 = {:.2}", p999);
    println!("  max   = {:.2}", max);
}

fn report_per_thread(workers: &[WorkerStats]) {
    println!("\n[Per-thread]");
    for (i, worker) in workers.iter().enumerate() {
        let throughput = if worker.elapsed.is_zero() {
            0.0
        } else {
            worker.ops as f64 / worker.elapsed.as_secs_f64()
        };
        println!("  worker {:>2}: {:.0} ops/sec (ops={})", i, throughput, worker.ops);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    args.threads = normalize_threads(args.threads);

    let _ = std::fs::remove_dir_all(&args.path);
    let options = build_options(&args);
    let storage = MiniLsm::open(&args.path, options).await?;
    let mut rng = StdRng::seed_from_u64(args.seed);

    let load_time = if args.threads == 1 {
        load_phase(&storage, args.record_count, args.value_size, &mut rng).await?
    } else {
        load_phase_parallel(&storage, args.record_count, args.value_size, args.threads).await?
    };

    if args.warmup_ops > 0 {
        println!(
            "[WARMUP] Running {} operations (workload {:?}) with {} workers...",
            args.warmup_ops, args.workload, args.threads
        );
        let (elapsed, _, _) = run_workload_parallel(&storage, &args, args.warmup_ops, false).await?;
        println!("[WARMUP] Done in {:?}", elapsed);
    }

    println!(
        "[RUN] Running {} operations (workload {:?}) with {} workers...",
        args.operation_count, args.workload, args.threads
    );
    let (run_time, mut latencies, workers) = run_workload_parallel(&storage, &args, args.operation_count, true).await?;

    report_latencies(&mut latencies);
    if args.report_per_thread {
        report_per_thread(&workers);
    }

    println!("\n[Summary]");
    println!("  Load throughput: {:.0} ops/sec", args.record_count as f64 / load_time.as_secs_f64());
    println!("  Run throughput:  {:.0} ops/sec", args.operation_count as f64 / run_time.as_secs_f64());

    storage.close().await?;
    let _ = std::fs::remove_dir_all(&args.path);

    Ok(())
}
