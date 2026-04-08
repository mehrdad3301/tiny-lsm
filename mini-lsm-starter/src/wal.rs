// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    /// ??? why put file behind a mutex
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(writer)),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf: &[u8] = buf.as_slice();
        while buf.has_remaining() {
            let batch_size = buf.get_u32() as usize;
            if buf.remaining() < batch_size {
                bail!("invalid batch")
            }

            let mut batch = &buf[..batch_size];
            let calculated_checksum = crc32fast::hash(batch);

            let mut data = Vec::new();
            while batch.has_remaining() {
                let key_len = batch.get_u16() as usize;
                let key = Bytes::copy_from_slice(&batch[..key_len]);
                batch.advance(key_len);
                let ts = batch.get_u64();
                let value_len = batch.get_u16() as usize;
                let value = Bytes::copy_from_slice(&batch[..value_len]);
                batch.advance(value_len);
                let key = KeyBytes::from_bytes_with_ts(key, ts);
                data.push((key, value));
            }

            buf.advance(batch_size);
            let checksum = buf.get_u32();
            if !calculated_checksum.eq(&checksum) {
                bail!("checksum mismatch");
            }
            for (key, value) in data {
                skiplist.insert(key, value);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::<u8>::new();
        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }
        // write batch_size header (u32)
        file.write_all(&(buf.len() as u32).to_be_bytes())?;
        // write key-value pairs body
        file.write_all(&buf)?;
        // write checksum (u32)
        file.write_all(&crc32fast::hash(&buf).to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
