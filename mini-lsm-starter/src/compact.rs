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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::{HashSet, hash_set};
use std::fmt::UpperExp;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result, anyhow};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use rand::seq::index::sample;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{Key, KeySlice};
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::mvcc::watermark;
use crate::table::{self, SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    /// ??? what is the following syntax ? vs StorageIterator
    async fn create_ssts_from_iter<I>(
        &self,
        mut iter: I,
        compact_to_bottom: bool,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + Send + 'static,
    {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut sstables = Vec::new();
        let mut prev_key = Vec::<u8>::new();
        let mut first_time_below_watermark = false;
        let watermark = self.mvcc().watermark();
        let compaction_filters = self.compaction_filters.lock().await;

        'outer: while iter.is_valid() {
            let same_as_prev = iter.key().key_ref() == prev_key;

            if !same_as_prev {
                first_time_below_watermark = true;
            }

            if iter.key().ts() <= watermark {
                if !first_time_below_watermark {
                    iter.next()?;
                    continue;
                }

                for filter in &compaction_filters.clone() {
                      match filter {
                          CompactionFilter::Prefix(filter) => {
                              if iter.key().key_ref().starts_with(&filter) {
                                    iter.next()?;
                                    continue 'outer;
                              }
                          },
                      }
                }

                if compact_to_bottom && iter.value().is_empty() {
                    first_time_below_watermark = false;
                    prev_key.clear();
                    prev_key.extend(iter.key().key_ref());
                    iter.next()?;
                    continue;
                }

                first_time_below_watermark = false;
            }

            builder.add(iter.key(), iter.value());

            if !same_as_prev && builder.estimated_size() > self.options.target_sst_size {
                let id = self.next_sst_id();
                let ready_builder =
                    std::mem::replace(&mut builder, SsTableBuilder::new(self.options.block_size));
                sstables.push(Arc::new(ready_builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                ).await?));
            }

            if !same_as_prev {
                prev_key.clear();
                prev_key.extend(iter.key().key_ref());
            }
            iter.next()?;
        }

        // create sstable from remaining elements
        if !builder.is_empty() {
            let id = self.next_sst_id();
            sstables.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            ).await?));
        }

        Ok(sstables)
    }

    async fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        match task {
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            })
            | CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                lower_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => {
                if let Some(_) = upper_level {
                    let mut sstables = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        sstables.push(table);
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(sstables).await?;

                    let mut sstables = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        sstables.push(table);
                    }

                    let lower_iter = SstConcatIterator::create_and_seek_to_first(sstables).await?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.create_ssts_from_iter(iter, task.compact_to_bottom_level()).await
                } else {
                    let mut iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for table in upper_level_sst_ids.iter() {
                        if let Some(table) = snapshot.sstables.get(table) {
                            iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                                Arc::clone(table),
                            ).await?));
                        }
                    }
                    let upper_iter = MergeIterator::create(iters);

                    let mut sstables = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        sstables.push(table);
                    }

                    let lower_iter = SstConcatIterator::create_and_seek_to_first(sstables).await?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.create_ssts_from_iter(iter, task.compact_to_bottom_level()).await
                }
            }

            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (tier_id, sstable_ids) in tiers {
                    let mut sstables = Vec::with_capacity(sstable_ids.len());
                    for id in sstable_ids {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        sstables.push(table);
                    }
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        sstables,
                    ).await?))
                }
                let iters = MergeIterator::create(iters);
                self.create_ssts_from_iter(iters, task.compact_to_bottom_level()).await
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut iters = Vec::with_capacity(l0_sstables.len());
                for table in l0_sstables.iter() {
                    if let Some(table) = snapshot.sstables.get(table) {
                        iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            Arc::clone(table),
                        ).await?));
                    }
                }
                let upper_iter = MergeIterator::create(iters);

                let mut sstables = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables {
                    let table = snapshot.sstables.get(id).unwrap().clone();
                    sstables.push(table);
                }

                let lower_iter = SstConcatIterator::create_and_seek_to_first(sstables).await?;

                let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                self.create_ssts_from_iter(iter, task.compact_to_bottom_level()).await
            }
        }
    }

    pub async fn force_full_compaction(&self) -> Result<()> {
        let ssts_to_compact = {
            let guard = self.state.read();
            (guard.l0_sstables.clone(), guard.levels[0].1.clone())
        }; // lock is dropped here

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: ssts_to_compact.0.clone(),
            l1_sstables: ssts_to_compact.1.clone(),
        };

        let generated_sstables = self.compact(&task).await?;
        let generated_sstable_ids = generated_sstables
            .iter()
            .map(|x| x.sst_id())
            .collect::<Vec<_>>();

        {
            let _lock = self.state_lock.lock().await;
            let mut snapshot = self.state.read().as_ref().clone();
            snapshot.levels[0].1 = Vec::with_capacity(generated_sstables.len());
            for table in generated_sstables {
                snapshot.levels[0].1.push(table.sst_id());
                snapshot.sstables.insert(table.sst_id(), table);
            }

            for table_id in ssts_to_compact.0.iter().chain(ssts_to_compact.1.iter()) {
                snapshot.sstables.remove(table_id);
            }

            // ??? why .copied() used in reference implementation
            let mut old_l0_ids = ssts_to_compact.0.iter().copied().collect::<HashSet<_>>();

            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .filter(|x| !old_l0_ids.remove(x))
                .copied()
                .collect();

            *self.state.write() = Arc::new(snapshot);
            self.sync_dir().await?;
            self.manifest.as_ref().unwrap().add_record(
                ManifestRecord::Compaction(task, generated_sstable_ids),
            ).await?;
        }

        for table_id in ssts_to_compact.0.iter().chain(ssts_to_compact.1.iter()) {
            tokio::fs::remove_file(self.path_of_sst(*table_id)).await?;
        }

        self.sync_dir().await?;

        Ok(())
    }

    async fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        if let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            let generated_sstables = self.compact(&task).await?;
            let generated_sstable_ids = generated_sstables
                .iter()
                .map(|x| x.sst_id())
                .collect::<Vec<_>>();

            let sstables_to_remove = {
                let _lock = self.state_lock.lock().await;
                let mut snapshot = self.state.read().as_ref().clone();
                // ??? what happens to the readers when we modify snapshot ???
                for table in generated_sstables {
                    let result = snapshot.sstables.insert(table.sst_id(), table);
                    assert!(result.is_none());
                }

                let (mut snapshot, sstables_to_remove) = self
                    .compaction_controller
                    .apply_compaction_result(&snapshot, &task, &generated_sstable_ids, false);

                for table_id in sstables_to_remove.iter() {
                    let result = snapshot.sstables.remove(table_id);
                    assert!(result.is_some());
                }

                *self.state.write() = Arc::new(snapshot);

                self.sync_dir().await?;
                self.manifest.as_ref().unwrap().add_record(
                    ManifestRecord::Compaction(task, generated_sstable_ids.clone()),
                ).await?;

                sstables_to_remove
            };

            println!(
                "compaction finished: {} files removed, {} files added, output={:?}",
                sstables_to_remove.len(),
                generated_sstable_ids.len(),
                generated_sstable_ids
            );

            for table_id in sstables_to_remove.iter() {
                tokio::fs::remove_file(self.path_of_sst(*table_id)).await?;
            }

            self.sync_dir().await?;
        }

        Ok(())
    }

    pub(crate) async fn spawn_compaction_task(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<Option<tokio::task::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(50));
                loop {
                    tokio::select! {
                        _ = interval.tick() => if let Err(e) = this.trigger_compaction().await {
                            eprintln!("compaction failed: {}", e);
                        },
                        _ = rx.recv() => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    async fn trigger_flush(&self) -> Result<()> {
        let imm_memtables_len = {
            let guard = self.state.read();
            guard.imm_memtables.len()
        }; // lock is dropped here

        if imm_memtables_len >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable().await?
        }

        Ok(())
    }

    pub(crate) async fn spawn_flush_task(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<Option<tokio::task::JoinHandle<()>>> {
        let this = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                    _ = interval.tick() => if let Err(e) = this.trigger_flush().await {
                        eprintln!("flush failed: {}", e);
                    },
                    _ = rx.recv() => return
                }
            }
        });
        Ok(Some(handle))
    }
}
