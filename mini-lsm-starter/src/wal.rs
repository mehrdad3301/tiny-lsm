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

use anyhow::{Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::key::{KeyBytes, KeySlice};

#[derive(Clone)]
pub struct Wal {
    file: Arc<Mutex<BufWriter<tokio::fs::File>>>,
    group_commit: Option<Arc<WalGroupCommit>>,
}

struct WalGroupCommit {
    write_tx: mpsc::Sender<WriteRequest>,
    _task: tokio::task::JoinHandle<()>,
}

struct WriteRequest {
    data: Vec<(KeyBytes, Bytes)>,
    notify: oneshot::Sender<Result<()>>,
}

impl Wal {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await?;
        let writer = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(writer)),
            group_commit: None,
        })
    }

    pub async fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
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
            group_commit: None,
        })
    }

    pub fn enable_group_commit(&mut self, timeout_ms: u64, max_batch: usize) {
        let (tx, rx) = mpsc::channel(max_batch * 2);
        let file = self.file.clone();
        let task = tokio::spawn(group_commit_loop(rx, file, timeout_ms, max_batch));
        self.group_commit = Some(Arc::new(WalGroupCommit {
            write_tx: tx,
            _task: task,
        }));
    }

    pub async fn put(&self, key: KeySlice<'_>, value: &[u8]) -> Result<()> {
        if let Some(ref gc) = self.group_commit {
            let key_bytes = key.to_key_vec().into_key_bytes();
            let data = vec![(key_bytes, Bytes::copy_from_slice(value))];
            gc.submit(data).await?;
        } else {
            self.put_batch(&[(key, value)]).await?;
        }
        Ok(())
    }

    pub async fn put_batch(&self, data: &[(KeySlice<'_>, &[u8])]) -> Result<()> {
        if let Some(ref gc) = self.group_commit {
            let batch_data: Vec<(KeyBytes, Bytes)> = data
                .iter()
                .map(|(k, v)| (k.to_key_vec().into_key_bytes(), Bytes::copy_from_slice(v)))
                .collect();
            gc.submit(batch_data).await?;
        } else {
            let mut file = self.file.lock().await;
            let mut buf = Vec::<u8>::new();
            for (key, value) in data {
                buf.put_u16(key.key_len() as u16);
                buf.put_slice(key.key_ref());
                buf.put_u64(key.ts());
                buf.put_u16(value.len() as u16);
                buf.put_slice(value);
            }
            file.write_all(&(buf.len() as u32).to_be_bytes()).await?;
            file.write_all(&buf).await?;
            file.write_all(&crc32fast::hash(&buf).to_be_bytes()).await?;
            file.flush().await?;
            file.get_mut().sync_all().await?;
        }
        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        if self.group_commit.is_some() {
            // Group commit handles sync automatically
            return Ok(());
        }
        let mut file = self.file.lock().await;
        file.flush().await?;
        file.get_mut().sync_all().await?;
        Ok(())
    }
}

impl WalGroupCommit {
    async fn submit(&self, data: Vec<(KeyBytes, Bytes)>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .send(WriteRequest { data, notify: tx })
            .await
            .map_err(|_| anyhow::anyhow!("group commit manager closed"))?;
        rx.await?
    }
}

async fn group_commit_loop(
    mut rx: mpsc::Receiver<WriteRequest>,
    file: Arc<Mutex<BufWriter<tokio::fs::File>>>,
    timeout_ms: u64,
    max_batch: usize,
) {
    let mut pending: Vec<WriteRequest> = Vec::new();
    let mut sleep = tokio::time::sleep(Duration::from_millis(timeout_ms));
    std::mem::forget(sleep);

    loop {
        sleep = tokio::time::sleep(Duration::from_millis(timeout_ms));
        tokio::pin!(sleep);

        tokio::select! {
            req = rx.recv() => {
                match req {
                    Some(req) => {
                        pending.push(req);
                        if pending.len() >= max_batch {
                            flush_batch(&file, &mut pending).await;
                        }
                    }
                    None => {
                        if !pending.is_empty() {
                            flush_batch(&file, &mut pending).await;
                        }
                        break;
                    }
                }
            }
            _ = &mut sleep => {
                if !pending.is_empty() {
                    flush_batch(&file, &mut pending).await;
                }
            }
        }
    }
}

async fn flush_batch(
    file: &Arc<Mutex<BufWriter<tokio::fs::File>>>,
    pending: &mut Vec<WriteRequest>,
) {
    let mut all_data: Vec<(KeySlice<'_>, &[u8])> = Vec::new();
    for req in pending.iter() {
        for (k, v) in req.data.iter() {
            let key = KeySlice::from_slice(k.key_ref(), k.ts());
            all_data.push((key, v.as_ref()));
        }
    }

    let success: Result<()> = async {
        let mut file = file.lock().await;
        let mut buf = Vec::<u8>::new();
        for (key, value) in &all_data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }
        if !buf.is_empty() {
            file.write_all(&(buf.len() as u32).to_be_bytes()).await?;
            file.write_all(&buf).await?;
            file.write_all(&crc32fast::hash(&buf).to_be_bytes()).await?;
            file.flush().await?;
            file.get_mut().sync_all().await?;
        }
        Ok(())
    }
    .await;

    for req in pending.drain(..) {
        let _ = req.notify.send(match &success {
            Ok(()) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        });
    }
}
