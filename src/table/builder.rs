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

use std::path::Path;
use std::sync::Arc;

use farmhash;

use anyhow::Result;
use bytes::BufMut;
use crc32fast;

use super::{BlockMeta, Bloom, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
    table::FileObject,
};

const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.01;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            key_hashes: Vec::new(),
            block_size,
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        // Track max timestamp
        if key.ts() > self.max_ts {
            self.max_ts = key.ts();
        }

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
            return;
        }

        self.add_block();

        assert!(self.builder.add(key, value));
        self.last_key.set_from_slice(key);
        self.first_key.set_from_slice(key);
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
    }

    fn add_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        let encoded = builder.build().encode();
        let uncompressed_len = encoded.len() as u32;
        let compressed = lz4_flex::compress(&encoded);
        // Format: [compressed_data][uncompressed_len: u32][crc32: u32]
        let checksum = crc32fast::hash(&compressed);
        self.data.extend(&compressed);
        self.data.put_u32(uncompressed_len);
        self.data.put_u32(checksum);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.builder.is_empty()
    }
    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub async fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.add_block();
        let meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data, self.max_ts);
        self.data.put_u32(meta_offset as u32);

        let bloom_filter = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), BLOOM_FILTER_FALSE_POSITIVE_RATE),
        );

        let bloom_filter_offset = self.data.len();
        bloom_filter.encode(&mut self.data);
        self.data.put_u32(bloom_filter_offset as u32);

        let file = FileObject::create(path.as_ref(), self.data).await?;
        SsTable::open(id, block_cache, file).await
    }

    #[cfg(test)]
    pub(crate) async fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path).await
    }
}
