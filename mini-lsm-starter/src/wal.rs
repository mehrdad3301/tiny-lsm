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
use std::hash::Hasher;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf: &[u8] = buf.as_slice();
        while buf.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = buf.get_u16() as usize;
            hasher.write_u16(key_len as u16) ;
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            hasher.write(&key) ;
            buf.advance(key_len);
            let value_len = buf.get_u16() as usize;
            hasher.write_u16(value_len as u16) ;
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            hasher.write(&value) ;
            buf.advance(value_len);
            let checksum = buf.get_u32() ; 
            if !hasher.finalize().eq(&checksum) { 
                bail!("checksum mismatch");
            }
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buf = vec![];
        let mut hasher = crc32fast::Hasher::new();
        buf.put_u16(key.len() as u16);
        hasher.write_u16(key.len() as u16) ; 
        buf.put(key);
        hasher.write(key) ;
        buf.put_u16(value.len() as u16);
        hasher.write_u16(value.len() as u16) ; 
        buf.put(value);
        hasher.write(value) ;
        let checksum = hasher.finalize() ; 
        buf.put_u32(checksum) ; 
        self.file.lock().write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
