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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::mem;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Context, Result};
use bytes::{Buf, Bytes};
use clap::error::ErrorKind;
use parking_lot::{Mutex, MutexGuard, RwLock};

use farmhash;

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= user_key && user_key <= table_end.raw_ref()
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?; /// ??? why 

        // wait for the flush thread to finish
        self.flush_notifier.send(())?;
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        // wait for the compaction thread to finish
        self.compaction_notifier.send(())?;
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // flush all memtables if wal disabled
        if !self.inner.state.read().memtable.is_empty() {
            {
                let lock = self.inner.state_lock.lock();
                self.inner.force_freeze_memtable(&lock)?;
            } // lock dropped
        }

        let mut remaining_tables = self.inner.state.read().imm_memtables.len();
        while remaining_tables != 0 {
            self.inner.force_flush_next_imm_memtable()?;
            remaining_tables -= 1;
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,
        let manifest;

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }

        let manifest_path = path.join(Path::new("MANIFEST"));
        if !manifest_path.exists() {
            manifest = Manifest::create(manifest_path).context("failed to create manifest")?;
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?)
            }
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            // recover db if manifest exists
            let (recovered_manifest, manifest_records) = Manifest::recover(manifest_path)?;
            manifest = recovered_manifest;
            let mut memtables = BTreeSet::new();
            for record in manifest_records {
                match record {
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, removed_files) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);

                        // try removing old ssts
                        for table_id in removed_files {
                            if let Err(err) =
                                std::fs::remove_file(Self::path_of_sst_static(path, table_id))
                            {
                                if err.kind() != std::io::ErrorKind::NotFound {
                                    return Err(err.into());
                                }
                            }
                        }

                        state = new_state;
                        next_sst_id = next_sst_id.max(output.iter().max().copied().unwrap())
                    },
                    ManifestRecord::Flush(id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                        next_sst_id = id.max(next_sst_id);
                        assert!(memtables.remove(&id), "missing manifest record for flushed memtable !");
                    },
                    ManifestRecord::NewMemtable(id) => {
                        next_sst_id = id.max(next_sst_id);
                        memtables.insert(id);
                    }
                }
            }

            // load ssts and fill sstables map
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let table = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))?,
                )?;

                state.sstables.insert(table_id, Arc::new(table));
            }

            next_sst_id += 1;

            if let CompactionOptions::Leveled(_) = &options.compaction_options {
                // sort the ssts
                for level in 0..state.levels.len() {
                    state.levels[level].1.sort_by(|x, y| {
                        state
                            .sstables
                            .get(x)
                            .unwrap()
                            .first_key()
                            .cmp(state.sstables.get(y).unwrap().first_key())
                    });
                }
            }

            // recover memtables
            if options.enable_wal {
                for memtable_id in memtables {
                    let memtable = MemTable::recover_from_wal(
                        memtable_id,
                        Self::path_of_wal_static(path, memtable_id),
                    )?;
                    if !memtable.is_empty() { 
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;

        return Ok(storage);
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // ??? why is lock dropped ??? 

        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // tombstone
                return Ok(None);
            }
            return Ok(Some(value));
        }

        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    // tombstone
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let key = KeySlice::from_slice(key);
        for sstable in snapshot.l0_sstables.iter() {
            let sstable = snapshot.sstables.get(sstable).unwrap();
            if !key_within(
                key.raw_ref(),
                sstable.first_key().as_key_slice(),
                sstable.last_key().as_key_slice(),
            ) {
                continue;
            }
            if let Some(bloom_filter) = &sstable.bloom {
                if !bloom_filter.may_contain(farmhash::fingerprint32(key.raw_ref())) {
                    continue;
                }
            }
            let iter = SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?;
            if iter.is_valid() && iter.key() == key {
                if iter.value().is_empty() {
                    // tombstone
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        for (level, sstable_ids) in snapshot.levels.iter() {
            let mut sstables = Vec::with_capacity(sstable_ids.len());
            for id in sstable_ids {
                let table = snapshot.sstables.get(id).unwrap().clone();
                if !key_within(
                    key.raw_ref(),
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    continue;
                }
                if let Some(bloom_filter) = &table.bloom {
                    if !bloom_filter.may_contain(farmhash::fingerprint32(key.raw_ref())) {
                        continue;
                    }
                }
                sstables.push(table);
            }
            let iter = SstConcatIterator::create_and_seek_to_key(sstables, key)?;
            if iter.is_valid() && iter.key() == key {
                if iter.value().is_empty() {
                    // tombstone
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        // value not found
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let guard = self.state.read();

        guard.memtable.put(key, value)?;

        if guard.memtable.approximate_size() > self.options.target_sst_size {
            drop(guard);
            let lock = self.state_lock.lock();
            let guard = self.state.read();
            // check again, another thread might have frozen memtable
            if guard.memtable.approximate_size() > self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, b"")
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // creating memtable before acquiring the lock
        let id = self.next_sst_id() ; 
        let memtable ;
        if self.options.enable_wal { 
            memtable = Arc::new(MemTable::create_with_wal(id, 
                self.path_of_wal(id))?)
        } else { 
            memtable = Arc::new(MemTable::create(id));
        }

        let frozen_memtable = {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let frozen_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
            snapshot.imm_memtables.insert(0, frozen_memtable.clone());
            *guard = Arc::new(snapshot);
            frozen_memtable
        } ;
        
        frozen_memtable.sync_wal()?;

        dbg!(frozen_memtable.is_empty());

        self.sync_dir()?;

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(
                &state_lock_observer,
            ManifestRecord::NewMemtable(id))?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        /// ??? why do we need state lock here ?
        let lock = self.state_lock.lock();
        let memtable = {
            let guard = self.state.read();
            guard
                .imm_memtables
                .last()
                .expect("no immutable memtables found during the flush")
                .clone()
        }; // lock is dropped here 

        let id = memtable.id();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut builder)?;
        let table = builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, id);
            } else {
                snapshot.levels.insert(0, (id, vec![id]));
            }
            snapshot.sstables.insert(id, Arc::new(table));
            snapshot.imm_memtables.pop();
            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(id))?;
        }

        self.sync_dir()?;

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&lock, ManifestRecord::Flush(id))?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // lock is dropped here 

        let mut memtable_iters: Vec<Box<MemTableIterator>> = Vec::new();
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));

        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }

        let mut l0_iters: Vec<Box<SsTableIterator>> = Vec::new();
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables.get(table_id).unwrap().clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                l0_iters.push(Box::new(iter));
            }
        }

        let mut level_iters = Vec::new();
        for (level, sstable_ids) in snapshot.levels.iter() {
            let mut sstables = Vec::with_capacity(sstable_ids.len());
            for id in sstable_ids {
                let table = snapshot.sstables.get(id).unwrap().clone();
                sstables.push(table);
            }
            let iter = match lower {
                Bound::Included(key) => {
                    SstConcatIterator::create_and_seek_to_key(sstables, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        sstables,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables)?,
            };
            level_iters.push(Box::new(iter));
        }

        let level_iters = MergeIterator::create(level_iters);

        let iters = TwoMergeIterator::create(
            MergeIterator::create(memtable_iters),
            MergeIterator::create(l0_iters),
        )?;

        let iters = LsmIterator::new(
            TwoMergeIterator::create(iters, level_iters)?,
            upper.map(|s| Bytes::copy_from_slice(s)),
        )?;

        Ok(FusedIterator::new(iters))
    }
}
