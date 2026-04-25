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

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::{Result, anyhow};

use crate::block::Block;

const INITIAL_READAHEAD: usize = 1;
const MAX_READAHEAD: usize = 4;
/// Sequential transitions before doubling readahead.
const SEQUENTIAL_GROW_THRESHOLD: usize = 2;

/// Manages background prefetching of SSTable blocks.
///
/// Maintains a queue of ready (already fetched) blocks and at most one in-flight
/// I/O handle producing a batch of blocks via `tokio::spawn`. The readahead depth
/// adapts: starts at 1 block, doubles on sequential access up to MAX_READAHEAD.
pub struct PrefetchBuffer {
    /// Blocks already fetched and ready for immediate consumption.
    /// Ordered by block_idx ascending: front = next to consume.
    ready_blocks: VecDeque<(usize, Arc<Block>)>,

    /// In-flight I/O handle producing a batch of blocks.
    inflight_handle: Option<tokio::task::JoinHandle<Result<Vec<Arc<Block>>>>>,

    /// Starting block index of the in-flight batch.
    inflight_start_idx: usize,

    /// Number of blocks in the in-flight batch.
    inflight_count: usize,

    /// Current readahead depth (number of blocks to prefetch ahead).
    readahead_depth: usize,

    /// Sequential block transitions observed since last reset.
    sequential_count: usize,
}

impl PrefetchBuffer {

    pub fn new() -> Self {
        Self {
            ready_blocks: VecDeque::new(),
            inflight_handle: None,
            inflight_start_idx: 0,
            inflight_count: 0,
            readahead_depth: INITIAL_READAHEAD,
            sequential_count: 0,
        }
    }

    /// Try to consume a prefetched block for `block_idx`.
    ///
    /// Returns:
    /// - `Some(Ok(block))` if the block was ready or fetched via the in-flight handle.
    /// - `Some(Err(..))` if the in-flight I/O failed.
    /// - `None` if no prefetch exists for this block_idx.
    pub fn try_take(&mut self, block_idx: usize) -> Option<Result<Arc<Block>>> {
        // Check ready_blocks first
        if self.ready_blocks.front().map(|(idx, _)| *idx) == Some(block_idx) {
            let (_, block) = self.ready_blocks.pop_front().unwrap();
            return Some(Ok(block));
        }

        // Check if in-flight handle covers this block
        if self.inflight_handle.is_some()
            && block_idx >= self.inflight_start_idx
            && block_idx < self.inflight_start_idx + self.inflight_count
        {
            return self.await_inflight(block_idx);
        }

        None
    }

    /// Await the in-flight handle and deposit remaining blocks into ready_blocks.
    fn await_inflight(&mut self, block_idx: usize) -> Option<Result<Arc<Block>>> {
        let handle = self.inflight_handle.take().unwrap();
        let start_idx = self.inflight_start_idx;
        self.inflight_count = 0;

        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(handle)
        });

        match result {
            Ok(Ok(blocks)) => {
                let mut target = None;
                for (i, block) in blocks.into_iter().enumerate() {
                    let idx = start_idx + i;
                    if idx == block_idx {
                        target = Some(block);
                    } else if idx > block_idx {
                        // Only deposit blocks that come after the target —
                        // blocks before it are stale (iterator already passed them).
                        self.ready_blocks.push_back((idx, block));
                    }
                }
                target.map(Ok)
            }
            Ok(Err(e)) => {
                self.ready_blocks.clear();
                Some(Err(e))
            }
            Err(join_err) => {
                self.ready_blocks.clear();
                Some(Err(anyhow!("prefetch task failed: {}", join_err)))
            }
        }
    }

    /// Record a sequential block transition. Grows readahead depth adaptively.
    pub fn record_sequential(&mut self) {
        self.sequential_count += 1;
        if self.sequential_count >= SEQUENTIAL_GROW_THRESHOLD
            && self.readahead_depth < MAX_READAHEAD
        {
            self.readahead_depth = (self.readahead_depth * 2).min(MAX_READAHEAD);
            self.sequential_count = 0;
        }
    }

    /// Record a non-sequential access. Resets sequential counter.
    pub fn record_non_sequential(&mut self) {
        self.sequential_count = 0;
    }

    /// Discard all in-flight prefetches and ready blocks. Called on seek.
    pub fn invalidate(&mut self) {
        if let Some(handle) = self.inflight_handle.take() {
            handle.abort();
        }
        self.inflight_count = 0;
        self.ready_blocks.clear();
        self.readahead_depth = INITIAL_READAHEAD;
        self.sequential_count = 0;
    }

    /// Current readahead depth.
    pub fn readahead_depth(&self) -> usize {
        self.readahead_depth
    }

    /// Store a launched prefetch handle. Does nothing if an inflight already exists.
    pub fn set_inflight(
        &mut self,
        start_idx: usize,
        count: usize,
        handle: tokio::task::JoinHandle<Result<Vec<Arc<Block>>>>,
    ) {
        if self.inflight_handle.is_some() {
            // Don't overwrite — let existing inflight complete first
            handle.abort();
            return;
        }
        self.inflight_start_idx = start_idx;
        self.inflight_count = count;
        self.inflight_handle = Some(handle);
    }

    /// Check if a block index is already covered by ready_blocks or inflight.
    pub fn is_covered(&self, block_idx: usize) -> bool {
        if self.ready_blocks.iter().any(|(idx, _)| *idx == block_idx) {
            return true;
        }
        if self.inflight_handle.is_some()
            && block_idx >= self.inflight_start_idx
            && block_idx < self.inflight_start_idx + self.inflight_count
        {
            return true;
        }
        false
    }
}

impl Default for PrefetchBuffer {
    fn default() -> Self {
        Self::new()
    }
}
