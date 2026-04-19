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
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use mini_lsm_starter::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
    TieredCompactionOptions,
};
use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
// use rand::distributions::Distribution;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

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

#[derive(Parser, Debug)]
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
    }
}

/// Zipfian distribution sampler for skewed key access.
struct Zipfian {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipfian {
    fn new(n: u64, theta: f64) -> Self {
        let zetan = (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        let alpha = 1.0 / (1.0 - theta);
        let eta = (2.0f64.powf(1.0 - theta) - 1.0) / (2.0f64.powf(alpha) - 1.0);
        Self { n, theta, alpha, zetan, eta }
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

async fn run_workload(
    storage: &Arc<MiniLsm>,
    args: &Args,
    rng: &mut StdRng,
) -> Result<(Duration, Vec<f64>)> {
    let op_count = args.operation_count;
    let record_count = args.record_count;
    let mut latencies = Vec::with_capacity(op_count as usize);

    let zipf = if args.zipfian {
        Some(Zipfian::new(record_count, 0.99))
    } else {
        None
    };

    let mut next_insert_id = record_count;

    println!("[RUN] Running {} operations (workload {:?})...", op_count, args.workload);
    let start = Instant::now();

    for _ in 0..op_count {
        let key_idx = match &zipf {
            Some(z) => z.sample(rng),
            None => rng.gen_range(0..record_count),
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
                    let read_key = make_key(rng.gen_range(0..next_insert_id));
                    storage.get(&read_key).await?;
                } else {
                    let insert_key = make_key(next_insert_id);
                    let value = make_value(args.value_size);
                    storage.put(&insert_key, &value).await?;
                    next_insert_id += 1;
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
                    let insert_key = make_key(next_insert_id);
                    let value = make_value(args.value_size);
                    storage.put(&insert_key, &value).await?;
                    next_insert_id += 1;
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
        latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0); // microseconds
    }

    let elapsed = start.elapsed();
    println!("[RUN] Done in {:?} ({:.0} ops/sec)", elapsed, op_count as f64 / elapsed.as_secs_f64());
    Ok((elapsed, latencies))
}

fn report_latencies(latencies: &mut [f64]) {
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let _ = std::fs::remove_dir_all(&args.path);
    let options = build_options(&args);
    let storage = MiniLsm::open(&args.path, options).await?;
    let mut rng = StdRng::seed_from_u64(args.seed);

    let load_time = load_phase(&storage, args.record_count, args.value_size, &mut rng).await?;
    let (run_time, mut latencies) = run_workload(&storage, &args, &mut rng).await?;

    report_latencies(&mut latencies);

    println!("\n[Summary]");
    println!("  Load throughput: {:.0} ops/sec", args.record_count as f64 / load_time.as_secs_f64());
    println!("  Run throughput:  {:.0} ops/sec", args.operation_count as f64 / run_time.as_secs_f64());

    storage.close().await?;
    let _ = std::fs::remove_dir_all(&args.path);

    Ok(())
}
