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

use crate::key::{KeySlice, KeyVec, TS_DEFAULT};
use bytes::BufMut;

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    fn key_overlap_len(&self, key: KeySlice) -> usize {
        let mut idx: usize = 0;

        while idx < self.first_key.key_len() && idx < key.key_len() {
            if self.first_key.key_ref()[idx] != key.key_ref()[idx] {
                break;
            }
            idx += 1;
        }
        idx
    }

    fn estimate_size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    /// ??? what happens if key/value length overflows
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        if self.estimate_size() + key.raw_len() + value.len() + SIZEOF_U16 * 3 // key, value, and offset 
        > self.block_size
            && !self.is_empty()
        {
            /// ??? why check for !self.is_empty()
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        let mut buf = vec![];

        let key_overlap_len = self.key_overlap_len(key);

        buf.put_u16(key_overlap_len as u16);

        buf.put_u16((key.key_len() - key_overlap_len) as u16);
        buf.put_slice(&key.key_ref()[key_overlap_len..]);

        buf.put_u64(key.ts());

        buf.put_u16(value.len() as u16);
        buf.put_slice(value);

        self.data.append(&mut buf);

        let len = buf.len();

        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
