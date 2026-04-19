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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_skiplist::{SkipMap, map::Entry};
use nom::AsBytes;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound, mvcc::CommittedTxnData,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("attempted to modify a commited transaction !");
        }

        if let Some(key_hashes) = &self.key_hashes {
            key_hashes
                .lock().1.insert(farmhash::hash32(key)) ;
        }

        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            }
            return Ok(Some(entry.value().clone()));
        }
        self.inner.get_with_ts(key, self.read_ts).await
    }

    pub async fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("attempted to modify a commited transaction !");
        }

        let mut iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iter.next()?;

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(iter, self.inner.scan_with_ts(lower, upper, self.read_ts).await?)?,
        ).await
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("attempted to modify a commited transaction !");
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        if let Some(key_hashes) = &self.key_hashes {
            key_hashes
                .lock().0.insert(farmhash::hash32(key)) ;
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, b"")
    }

    pub async fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");

        // take the commit lock
        let _lock = self.inner.mvcc().commit_lock.lock().await;

        let commit_ts = self.inner.mvcc().latest_commit_ts() + 1 ;

        // check serializability
        if self.inner.options.serializable {
            if let Some(key_hashes) = &self.key_hashes {
                let (write_set, read_set) = &*key_hashes.lock() ;
                let commited_txn = self.inner.mvcc().committed_txns.lock().await ;
                if write_set.is_empty() {
                    return Ok(()) ;
                }

                for (_, write_set) in commited_txn.range(
                    (Bound::Excluded(self.read_ts), Bound::Excluded(commit_ts))) {
                    for keys in write_set.key_hashes.iter() {
                        if read_set.contains(keys) {
                            return Err(anyhow::anyhow!("serialization failure due to concurrent write-write conflict!"))
                        }
                    }
                }
            } else {
                panic!("serializability check is enabled but key_hashes is not initialized!") ;
            }
        }

        // write commit batch
        let batch = self
            .local_storage
            .iter()
            .map(|x| {
                if x.value().is_empty() {
                    WriteBatchRecord::Del(x.key().clone())
                } else {
                    WriteBatchRecord::Put(x.key().clone(), x.value().clone())
                }
            })
            .collect::<Vec<_>>();

        self.inner.write_batch_inner(&batch).await?;

        if self.inner.options.serializable {

            // add transaction data to mvcc
            let mut commited_txn = self.inner.mvcc().committed_txns.lock().await ;
            commited_txn.insert(commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(&mut self.key_hashes.as_ref().unwrap().lock().0),
                    read_ts: self.read_ts,
                    commit_ts
                }
            );

            // garbage collect committed transactions
            let watermark = self.inner.mvcc().watermark() ;
            while let Some(entry) = commited_txn.first_entry() {
                if entry.get().commit_ts <= watermark {
                    entry.remove() ;
                } else {
                    break ;
                }
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn map(next: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        match next {
            Some(entry) => return (entry.key().clone(), entry.value().clone()),
            None => return (Bytes::new(), Bytes::new()),
        }
    }
}

#[async_trait]
impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_bytes()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_bytes()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let item = self.with_iter_mut(|iter| TxnLocalIterator::map(iter.next()));
        self.with_mut(|x| *x.item = item);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub async fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut txn_iter = Self { txn, iter };
        txn_iter.move_to_non_delete_key()?;
        txn_iter.add_to_read_set();
        Ok(txn_iter)
    }

    fn move_to_non_delete_key(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&self) {
        if self.is_valid() {
            if let Some(key_hashes) = &self.txn.key_hashes {
                key_hashes.lock().1.insert(farmhash::hash32(self.key())) ;
            }
        }
    }
}

#[async_trait]
impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.move_to_non_delete_key()?;
        self.add_to_read_set();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
