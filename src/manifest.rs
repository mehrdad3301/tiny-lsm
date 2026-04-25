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

use anyhow::{Result, bail};
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<tokio::fs::File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .await?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .await?;

        let mut buf = vec![];
        file.read_to_end(&mut buf).await?;
        let mut buf = buf.as_slice();

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

    pub async fn add_record(&self, record: ManifestRecord) -> Result<()> {
        self.add_record_when_init(record).await
    }

    pub async fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock().await;
        let mut buf = vec![];
        let record = serde_json::to_vec(&record)?;
        let checksum = crc32fast::hash(&record);
        buf.put_u64(record.len() as u64);
        buf.put(&record[..]);
        buf.put_u32(checksum);
        file.write_all(&buf).await?;
        file.sync_all().await?;
        Ok(())
    }
}
