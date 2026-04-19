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

use std::sync::Arc;

use tempfile::tempdir;

use crate::{
    lsm_storage::{LsmStorageInner, LsmStorageOptions},
    mem_table::MemTable,
};

#[tokio::test]
async fn test_task1_memtable_get() {
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").await.unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").await.unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").await.unwrap();
    assert_eq!(
        &memtable.for_testing_get_slice(b"key1").unwrap()[..],
        b"value1"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key2").unwrap()[..],
        b"value2"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key3").unwrap()[..],
        b"value3"
    );
}

#[tokio::test]
async fn test_task1_memtable_overwrite() {
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").await.unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").await.unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").await.unwrap();
    memtable.for_testing_put_slice(b"key1", b"value11").await.unwrap();
    memtable.for_testing_put_slice(b"key2", b"value22").await.unwrap();
    memtable.for_testing_put_slice(b"key3", b"value33").await.unwrap();
    assert_eq!(
        &memtable.for_testing_get_slice(b"key1").unwrap()[..],
        b"value11"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key2").unwrap()[..],
        b"value22"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key3").unwrap()[..],
        b"value33"
    );
}

#[tokio::test]
async fn test_task2_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).await.unwrap(),
    );
    assert_eq!(&storage.get(b"0").await.unwrap(), &None);
    storage.put(b"1", b"233").await.unwrap();
    storage.put(b"2", b"2333").await.unwrap();
    storage.put(b"3", b"23333").await.unwrap();
    assert_eq!(&storage.get(b"1").await.unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").await.unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").await.unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").await.unwrap();
    assert!(storage.get(b"2").await.unwrap().is_none());
    storage.delete(b"0").await.unwrap(); // should NOT report any error
}

#[tokio::test]
async fn test_task3_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).await.unwrap(),
    );
    storage.put(b"1", b"233").await.unwrap();
    storage.put(b"2", b"2333").await.unwrap();
    storage.put(b"3", b"23333").await.unwrap();
    storage
        .force_freeze_memtable().await
        .unwrap();
    assert_eq!(storage.state.read().imm_memtables.len(), 1);
    let previous_approximate_size = storage.state.read().imm_memtables[0].approximate_size();
    assert!(previous_approximate_size >= 15);
    storage.put(b"1", b"2333").await.unwrap();
    storage.put(b"2", b"23333").await.unwrap();
    storage.put(b"3", b"233333").await.unwrap();
    storage
        .force_freeze_memtable().await
        .unwrap();
    assert_eq!(storage.state.read().imm_memtables.len(), 2);
    assert!(
        storage.state.read().imm_memtables[1].approximate_size() == previous_approximate_size,
        "wrong order of memtables?"
    );
    assert!(storage.state.read().imm_memtables[0].approximate_size() > previous_approximate_size);
}

#[tokio::test]
async fn test_task3_freeze_on_capacity() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week1_test();
    options.target_sst_size = 1024;
    options.num_memtable_limit = 1000;
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).await.unwrap());
    for _ in 0..1000 {
        storage.put(b"1", b"2333").await.unwrap();
    }
    let num_imm_memtables = storage.state.read().imm_memtables.len();
    assert!(num_imm_memtables >= 1, "no memtable frozen?");
    for _ in 0..1000 {
        storage.delete(b"1").await.unwrap();
    }
    assert!(
        storage.state.read().imm_memtables.len() > num_imm_memtables,
        "no more memtable frozen?"
    );
}

#[tokio::test]
async fn test_task4_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).await.unwrap(),
    );
    assert_eq!(&storage.get(b"0").await.unwrap(), &None);
    storage.put(b"1", b"233").await.unwrap();
    storage.put(b"2", b"2333").await.unwrap();
    storage.put(b"3", b"23333").await.unwrap();
    storage
        .force_freeze_memtable().await
        .unwrap();
    storage.delete(b"1").await.unwrap();
    storage.delete(b"2").await.unwrap();
    storage.put(b"3", b"2333").await.unwrap();
    storage.put(b"4", b"23333").await.unwrap();
    storage
        .force_freeze_memtable().await
        .unwrap();
    storage.put(b"1", b"233333").await.unwrap();
    storage.put(b"3", b"233333").await.unwrap();
    assert_eq!(storage.state.read().imm_memtables.len(), 2);
    assert_eq!(&storage.get(b"1").await.unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"2").await.unwrap(), &None);
    assert_eq!(&storage.get(b"3").await.unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"4").await.unwrap().unwrap()[..], b"23333");
}
