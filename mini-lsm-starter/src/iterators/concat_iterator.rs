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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::{KeyBytes, KeySlice},
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key());
        }
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                assert!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
    }

    pub async fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        SstConcatIterator::check_sst_valid(&sstables);

        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            ).await?),
            next_sst_idx: 1,
            sstables,
        };

        if !iter.is_valid() {
            iter.next()?;
        }

        Ok(iter)
    }

    pub async fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice<'_>) -> Result<Self> {
        SstConcatIterator::check_sst_valid(&sstables);

        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let idx = sstables
            .partition_point(|x| x.first_key() <= &key.to_key_vec().into_key_bytes())
            .saturating_sub(1);

        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }

        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            ).await?),
            next_sst_idx: idx + 1,
            sstables,
        };

        if !iter.is_valid() {
            iter.next()?;
        }

        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = self.current.as_ref() {
            return current.is_valid();
        }
        false
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        while !self.is_valid() {
            if self.next_sst_idx < self.sstables.len() {
                // Note: This is still async internally but we call it from sync next()
                // The SsTableIterator will handle block loading
                let table = self.sstables[self.next_sst_idx].clone();
                self.current = Some(SsTableIterator::create_and_seek_to_first_sync(table)?);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
                break;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
