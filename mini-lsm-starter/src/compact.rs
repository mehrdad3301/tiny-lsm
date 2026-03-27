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
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
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
    fn create_ssts_from_iter(
        &self, 
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>, 
        compact_to_bottom: bool, 
    ) -> Result<Vec<Arc<SsTable>>> { 
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut sstables = Vec::new();

        while iter.is_valid() {
            if compact_to_bottom { 
                if !iter.value().is_empty() {
                    builder.add(iter.key(), iter.value());
                }
            } else { 
                builder.add(iter.key(), iter.value());
            }
            if builder.estimated_size() > self.options.target_sst_size {
                let id = self.next_sst_id();
                let ready_builder = std::mem::replace(
                    &mut builder,
                    SsTableBuilder::new(self.options.block_size),
                );
                sstables.push(Arc::new(ready_builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));
            }
            iter.next()?;
        }

        // create sstable from remaining elements
        let id = self.next_sst_id();
        sstables.push(Arc::new(builder.build(
            id,
            Some(self.block_cache.clone()),
            self.path_of_sst(id),
        )?));

        Ok(sstables)

    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        match task {
            CompactionTask::Simple(SimpleLeveledCompactionTask{ 
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
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(sstables)?;

                    let mut sstables = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        sstables.push(table);
                    }

                    let lower_iter = SstConcatIterator::create_and_seek_to_first(sstables)?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)? ;
                    self.create_ssts_from_iter(iter, task.compact_to_bottom_level())

                } else { 

                    let mut iters = Vec::with_capacity(upper_level_sst_ids.len()) ; 
                    for table in upper_level_sst_ids.iter() {
                        if let Some(table) = snapshot.sstables.get(table) {
                            iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                                Arc::clone(table),
                            )?));
                        }
                    }
                    let upper_iter = MergeIterator::create(iters) ; 

                    let mut sstables = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        sstables.push(table);
                    }

                    let lower_iter = SstConcatIterator::create_and_seek_to_first(sstables)?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)? ;
                    self.create_ssts_from_iter(iter, task.compact_to_bottom_level())

                } 
            }

            CompactionTask::Tiered(TieredCompactionTask{ 
                tiers, 
                ..
            }) => { 
                let mut iters = Vec::with_capacity(tiers.len()) ; 
                for (tier_id, sstable_ids) in tiers { 
                    let mut sstables = Vec::with_capacity(sstable_ids.len());
                    for id in sstable_ids {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        sstables.push(table);
                    }  
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(sstables)?))
                }
                let iters = MergeIterator::create(iters) ;
                self.create_ssts_from_iter(iters, task.compact_to_bottom_level()) 
            },
            CompactionTask::Leveled(_) => unimplemented!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {

                let mut iters = Vec::with_capacity(l0_sstables.len()) ; 
                for table in l0_sstables.iter() {
                    if let Some(table) = snapshot.sstables.get(table) {
                        iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            Arc::clone(table),
                        )?));
                    }
                }
                let upper_iter = MergeIterator::create(iters) ; 

                let mut sstables = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables {
                    let table = snapshot.sstables.get(id).unwrap().clone();
                    sstables.push(table);
                }

                let lower_iter = SstConcatIterator::create_and_seek_to_first(sstables)?;

                let iter = TwoMergeIterator::create(upper_iter, lower_iter)? ;
                self.create_ssts_from_iter(iter, task.compact_to_bottom_level())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let ssts_to_compact = {
            let guard = self.state.read();
            (guard.l0_sstables.clone(), guard.levels[0].1.clone())
        }; // lock is dropped here 

        let l1_sstables = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: ssts_to_compact.0.clone(),
            l1_sstables: ssts_to_compact.1.clone(),
        })?;

        {
            let lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            snapshot.levels[0].1 = Vec::with_capacity(l1_sstables.len());
            for table in l1_sstables {
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
        }

        for table_id in ssts_to_compact.0.iter().chain(ssts_to_compact.1.iter()) {
            std::fs::remove_file(self.path_of_sst(*table_id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        }; 
        
        if let Some(task) = self.compaction_controller.generate_compaction_task(&snapshot) { 
            let generated_sstables = self.compact(&task)? ; 
            let generated_sstable_ids = generated_sstables
                .iter()
                .map(|x| x.sst_id())
                .collect::<Vec<_>>() ; 

            let sstables_to_remove = {
                let lock = self.state_lock.lock();
                let snapshot = self.state.read().as_ref().clone();

                let (mut snapshot, sstables_to_remove) = self.compaction_controller
                    .apply_compaction_result(&snapshot, &task, &generated_sstable_ids, false) ;

                    for table in generated_sstables {
                        let result = snapshot.sstables.insert(table.sst_id(), table);
                        assert!(result.is_none()) ;
                    }

                    for table_id in sstables_to_remove.iter() {
                        let result = snapshot.sstables.remove(table_id);
                        assert!(result.is_some()) ;
                    }

                    *self.state.write() = Arc::new(snapshot);

                sstables_to_remove
            } ;

            println!(
                "compaction finished: {} files removed, {} files added, output={:?}",
                sstables_to_remove.len(),
                generated_sstable_ids.len(),
                generated_sstable_ids
            );

            for table_id in sstables_to_remove.iter() {
                std::fs::remove_file(self.path_of_sst(*table_id))?;
            }

        }
        
        Ok(())
    
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let imm_memtables_len = {
            let guard = self.state.read();
            guard.imm_memtables.len()
        }; // lock is dropped here 

        if imm_memtables_len >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
