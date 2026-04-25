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

use std::time::Duration;

use tempfile::tempdir;

use crate::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
    tests::harness::dump_files_in_dir,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_task3_compaction_integration() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.enable_wal = true;
    let storage = MiniLsm::open(&dir, options.clone()).await.unwrap();
    let _txn = storage.new_txn().unwrap();
    for i in 0..=20000 {
        storage
            .put(b"0", format!("{:02000}", i).as_bytes())
            .await
            .unwrap();
    } 

    {
        let _ = storage.inner.state_lock.lock() ;
        while {
            let snapshot = storage.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            storage.inner.force_flush_next_imm_memtable().await.unwrap();
        }
    }

    assert!(storage.inner.state.read().l0_sstables.len() > 1);
    storage.force_full_compaction().await.unwrap();
    storage.dump_structure();
    dump_files_in_dir(&dir);
    assert!(storage.inner.state.read().l0_sstables.is_empty());
    assert_eq!(storage.inner.state.read().levels.len(), 1);
    // same key in the same SST
    assert_eq!(storage.inner.state.read().levels[0].1.len(), 1);
    for i in 0..=100 {
        storage
            .put(b"1", format!("{:02000}", i).as_bytes())
            .await
            .unwrap();
    }

    storage
        .inner
        .force_freeze_memtable()
        .await
        .unwrap();

    {
        let _lock = storage.inner.state_lock.lock() ; 
        while {
            let snapshot = storage.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            storage.inner.force_flush_next_imm_memtable().await.unwrap();
        }
    }
    storage.force_full_compaction().await.unwrap();
    storage.dump_structure();
    dump_files_in_dir(&dir);
    assert!(storage.inner.state.read().l0_sstables.is_empty());
    assert_eq!(storage.inner.state.read().levels.len(), 1);
    assert_eq!(storage.inner.state.read().levels[0].1.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_trivial_move() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(
        LeveledCompactionOptions {
            level_size_multiplier: 2,
            level0_file_num_compaction_trigger: 2,
            max_levels: 4,
            base_level_size_mb: 0, // base level = L1 so inter-level compactions happen
        },
    ));
    options.target_sst_size = 4096; // small SSTs to create many levels
    let storage = MiniLsm::open(&dir, options.clone()).await.unwrap();

    // Build SSTs with non-overlapping key ranges in separate memtable freezes.
    // Each batch writes to a unique key, so SSTs have non-overlapping ranges.
    for key_idx in 0..10u8 {
        let key = vec![b'a' + key_idx];
        for i in 0..=50 {
            storage
                .put(&key, format!("v{}", i).as_bytes())
                .await
                .unwrap();
        }
        storage.inner.force_freeze_memtable().await.unwrap();
    }

    // Flush all imm memtables to L0
    {
        let _lock = storage.inner.state_lock.lock();
        while {
            let snapshot = storage.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            storage
                .inner
                .force_flush_next_imm_memtable()
                .await
                .unwrap();
        }
    }

    // Let background compaction task handle it — wait for convergence
    tokio::time::sleep(Duration::from_secs(2)).await;

    storage.dump_structure();

    // Verify all data intact
    for key_idx in 0..10u8 {
        let key = vec![b'a' + key_idx];
        assert_eq!(
            &storage.get(&key).await.unwrap().unwrap()[..],
            b"v50",
            "key '{}' mismatch",
            key_idx
        );
    }

    // Close + reopen to test manifest recovery
    storage.close().await.unwrap();
    drop(storage);

    let storage = MiniLsm::open(&dir, options).await.unwrap();
    for key_idx in 0..10u8 {
        let key = vec![b'a' + key_idx];
        assert_eq!(
            &storage.get(&key).await.unwrap().unwrap()[..],
            b"v50",
            "key '{}' mismatch after recovery",
            key_idx
        );
    }
}
