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

use std::fs::OpenOptions;
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Read};

use anyhow::{Result, bail};
use bytes::{Buf, BufMut, BytesMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // open manifest file
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();

        // decode manifest records
        let mut manifest_records = vec![];
        while buf.has_remaining() {
            let len = buf.get_u64() as usize;
            let manifest_record_bytes = &buf[..len];
            let manifest_record = serde_json::from_slice::<ManifestRecord>(&buf[..len]).unwrap();
            buf.advance(len);
            if crc32fast::hash(manifest_record_bytes) != buf.get_u32() {
                bail!("checksum mismatched!")
            }
            manifest_records.push(manifest_record);
        }

        let manifest = Self {
            file: Arc::new(Mutex::new(file)),
        };

        Ok((manifest, manifest_records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = vec![];
        let record = serde_json::to_vec(&record)?;
        let checksum = crc32fast::hash(&record);
        buf.put_u64(record.len() as u64);
        buf.put(&record[..]);
        buf.put_u32(checksum);
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
