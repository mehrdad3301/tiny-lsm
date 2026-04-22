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

use std::sync::Arc;

use anyhow::Result;

use super::prefetch::PrefetchBuffer;
use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    prefetch: PrefetchBuffer,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub async fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut iter = Self {
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0).await?),
            table,
            blk_idx: 0,
            prefetch: PrefetchBuffer::new(),
        };
        iter.launch_prefetch();
        Ok(iter)
    }

    pub fn create_and_seek_to_first_sync(table: Arc<SsTable>) -> Result<Self> {
        let block = tokio::task::block_in_place(|| {
            futures::executor::block_on(table.read_block(0))
        })?;
        let mut iter = Self {
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            table,
            blk_idx: 0,
            prefetch: PrefetchBuffer::new(),
        };
        iter.launch_prefetch();
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.prefetch.invalidate();
        self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0).await?);
        self.blk_idx = 0;
        self.launch_prefetch();
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub async fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice<'_>) -> Result<Self> {
        let mut blk_idx = table.find_block_idx(key);
        let mut blk_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx).await?, key);

        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < table.num_of_blocks() {
                blk_iter =
                    BlockIterator::create_and_seek_to_first(table.read_block_cached(blk_idx).await?);
            }
        }

        let mut iter = Self {
            table,
            blk_idx,
            blk_iter,
            prefetch: PrefetchBuffer::new(),
        };
        iter.launch_prefetch();
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub async fn seek_to_key(&mut self, key: KeySlice<'_>) -> Result<()> {
        self.prefetch.invalidate();
        self.blk_idx = self.table.find_block_idx(key);
        self.blk_iter =
            BlockIterator::create_and_seek_to_key(self.table.read_block_cached(self.blk_idx).await?, key);

        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx).await?,
                );
            }
        }
        self.launch_prefetch();
        Ok(())
    }

    /// Launch background prefetch for blocks after the current position.
    fn launch_prefetch(&mut self) {
        let start = self.blk_idx + 1;
        if start >= self.table.num_of_blocks() {
            return;
        }

        let count = self.prefetch.readahead_depth();
        let end = (start + count).min(self.table.num_of_blocks());

        // Skip if all blocks in range are already covered
        let all_covered = (start..end).all(|idx| self.prefetch.is_covered(idx));
        if all_covered {
            return;
        }

        // Find first uncovered block
        let actual_start = match (start..end).find(|idx| !self.prefetch.is_covered(*idx)) {
            Some(s) => s,
            None => return,
        };
        let actual_count = end - actual_start;

        let table = self.table.clone();
        let join_handle = tokio::spawn(async move {
            table.read_blocks_range(actual_start, actual_count).await
        });

        self.prefetch.set_inflight(actual_start, actual_count, join_handle);
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        let prev_blk_idx = self.blk_idx;
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                // Detect sequential access for adaptive readahead
                if self.blk_idx == prev_blk_idx + 1 {
                    self.prefetch.record_sequential();
                } else {
                    self.prefetch.record_non_sequential();
                }

                // Try consuming from prefetch buffer first
                let blk = match self.prefetch.try_take(self.blk_idx) {
                    Some(result) => result?,
                    None => {
                        // Fallback: synchronous read (missed prefetch or after seek)
                        tokio::task::block_in_place(|| {
                            futures::executor::block_on(self.table.read_block(self.blk_idx))
                        })?
                    }
                };

                self.blk_iter = BlockIterator::create_and_seek_to_first(blk);

                // Launch next prefetch after transitioning
                self.launch_prefetch();
            }
        }
        Ok(())
    }
}
