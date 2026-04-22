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

pub(crate) mod bloom;
mod builder;
mod iterator;
mod prefetch;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::{Block, SIZEOF_U32, SIZEOF_U64};
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    /// Format: [num_blocks][offset,first_key,last_key...][checksum][max_ts]
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>, max_ts: u64) {
        buf.put_u32(block_meta.len() as u32);
        let offset = buf.len();
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.key_len() as u16);
            buf.put_slice(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            buf.put_u16(meta.last_key.key_len() as u16);
            buf.put_slice(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
        buf.put_u64(max_ts);
        let checksum = crc32fast::hash(&buf[offset..]);
        buf.put_u32(checksum);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<(Vec<BlockMeta>, u64)> {
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - SIZEOF_U32]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(first_key_len), buf.get_u64());
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(last_key_len), buf.get_u64());
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        let max_ts = buf.get_u64();
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }

        Ok((block_meta, max_ts))
    }
}

/// A file object.
pub struct FileObject(Option<tokio::fs::File>, u64);

impl FileObject {
    pub async fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let file = self.0.as_ref().unwrap();
        let std_file = file.try_clone().await?.into_std().await;
        let mut data = vec![0; len as usize];
        tokio::task::spawn_blocking(move || {
            std_file.read_exact_at(&mut data, offset)?;
            Ok(data)
        }).await?
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub async fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(path)
            .await?;
        let size = file.metadata().await?.len();
        Ok(FileObject(Some(file), size))
    }

    pub async fn open(path: &Path) -> Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(path)
            .await?;
        let size = file.metadata().await?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) async fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file).await
    }

    /// Open SSTable from a file.
    pub async fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let size_u32 = SIZEOF_U32 as u64;
        let bloom_filter_offset = file
            .read(len - size_u32, size_u32)
            .await?
            .as_slice()
            .get_u32() as u64;

        let bloom_filter = Bloom::decode(
            file.read(bloom_filter_offset, len - bloom_filter_offset - size_u32)
                .await?
                .as_slice(),
        )?;

        let block_meta_offset = file
            .read(bloom_filter_offset - size_u32, size_u32)
            .await?
            .as_slice()
            .get_u32() as u64;

        let raw_meta = file.read(
            block_meta_offset,
            bloom_filter_offset - block_meta_offset - size_u32,
        ).await?;

        let (block_meta, max_ts) = BlockMeta::decode_block_meta(raw_meta.as_slice())?;

        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta_offset: block_meta_offset as usize,
            block_cache,
            id,
            block_meta,
            bloom: Some(bloom_filter),
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub async fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let offset_next = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);

        let data = self
            .file
            .read(offset as u64, (offset_next - offset) as u64)
            .await?;

        let block_len = offset_next - offset - 4;
        let block = &data[..(data.len() - SIZEOF_U32)];
        let checksum = (&data[(data.len() - SIZEOF_U32)..]).get_u32();
        if !crc32fast::hash(block).eq(&checksum) {
            return Err(anyhow!("corrupted block"));
        }

        let block = Block::decode(block);
        Ok(Arc::new(block))
    }

    /// Read multiple consecutive blocks in a single I/O operation for prefetching.
    pub async fn read_blocks_range(
        &self,
        start_idx: usize,
        count: usize,
    ) -> Result<Vec<Arc<Block>>> {
        if start_idx >= self.num_of_blocks() || count == 0 {
            return Ok(Vec::new());
        }

        let end_idx = (start_idx + count).min(self.num_of_blocks());
        let start_offset = self.block_meta[start_idx].offset as u64;
        let end_offset = self
            .block_meta
            .get(end_idx)
            .map_or(self.block_meta_offset as u64, |m| m.offset as u64);

        if end_offset <= start_offset {
            return Ok(Vec::new());
        }

        let data = self.file.read(start_offset, end_offset - start_offset).await?;

        let mut blocks = Vec::with_capacity(end_idx - start_idx);
        let mut cursor = 0usize;
        for i in start_idx..end_idx {
            let block_size = self
                .block_meta
                .get(i + 1)
                .map_or(self.block_meta_offset, |m| m.offset)
                - self.block_meta[i].offset;

            let block_data = &data[cursor..cursor + block_size];
            let block_raw = &block_data[..block_data.len() - SIZEOF_U32];
            let checksum = (&block_data[block_data.len() - SIZEOF_U32..]).get_u32();
            if !crc32fast::hash(block_raw).eq(&checksum) {
                return Err(anyhow!("corrupted block during prefetch at index {}", i));
            }

            blocks.push(Arc::new(Block::decode(block_raw)));
            cursor += block_size;
        }

        Ok(blocks)
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub async fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            let block = cache
                .try_get_with((self.id, block_idx), self.read_block(block_idx))
                .await
                .map_err(|e| anyhow!("{}", e))?;
            Ok(block)
        } else {
            self.read_block(block_idx).await
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
