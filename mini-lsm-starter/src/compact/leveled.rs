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

use std::collections::HashSet;

use crossbeam_skiplist::base;
use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let mut overlapping_ids = Vec::new();

        let first_key = sst_ids
            .iter()
            .map(|x| snapshot.sstables.get(x).unwrap().first_key())
            .min()
            .clone()
            .unwrap();

        let last_key = sst_ids
            .iter()
            .map(|x| snapshot.sstables.get(x).unwrap().last_key())
            .max()
            .clone()
            .unwrap();

        let in_level_tables = snapshot.levels[in_level - 1]
            .1
            .iter()
            .map(|x| snapshot.sstables.get(x).unwrap())
            .collect::<Vec<_>>();

        for table in in_level_tables.iter() {
            if table.first_key() <= last_key && table.last_key() >= first_key {
                overlapping_ids.push(table.sst_id());
            }
        }

        overlapping_ids
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // compute target sizes
        let mut target_level_sizes = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();

        let actual_level_sizes = snapshot
            .levels
            .iter()
            .map(|x| {
                x.1.iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>() as usize
            })
            .collect::<Vec<_>>();

        let base_level_size = self.options.base_level_size_mb * 1024 * 1024;

        target_level_sizes[self.options.max_levels - 1] = std::cmp::max(
            actual_level_sizes[self.options.max_levels - 1],
            base_level_size,
        );

        let mut base_level = self.options.max_levels;
        for level in (0..(self.options.max_levels - 1)).rev() {
            let next_level = level + 1;
            let this_level_size =
                target_level_sizes[next_level] / self.options.level_size_multiplier;
            if target_level_sizes[next_level] > base_level_size {
                target_level_sizes[level] = this_level_size;
            }
            if target_level_sizes[level] > 0 {
                base_level = level + 1;
            }
        }

        // flush l0 if necessary
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        // compute and sort priorities
        let priorities = actual_level_sizes
            .iter()
            .enumerate()
            .map(|(i, size)| (*size as f64 / target_level_sizes[i] as f64, i + 1))
            .filter(|x| x.0 > 1.0)
            .collect::<Vec<_>>();

        // get the top priority level
        let (ratio, top_priority_level) =
            priorities.iter().max_by(|a, b| a.partial_cmp(b).unwrap())?;

        let top_priority_level = *top_priority_level;

        // top priority level can never be max level since max level ratio is always 1
        assert!(top_priority_level < self.options.max_levels);

        let sstable_to_compact_id = snapshot.levels[top_priority_level - 1]
            .1
            .iter()
            .min()
            .copied()
            .unwrap();

        return Some(LeveledCompactionTask {
            upper_level: Some(top_priority_level),
            upper_level_sst_ids: vec![sstable_to_compact_id],
            lower_level: top_priority_level + 1,
            lower_level_sst_ids: self.find_overlapping_ssts(
                snapshot,
                &[sstable_to_compact_id],
                top_priority_level + 1,
            ),
            is_lower_level_bottom_level: top_priority_level + 1 == self.options.max_levels,
        });
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut upper_ids_to_remove = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        // remove compacted upper level sst ids
        match task.upper_level {
            Some(upper_level) => {
                snapshot.levels[upper_level - 1].1 = snapshot.levels[upper_level - 1]
                    .1
                    .iter()
                    .copied()
                    .filter(|x| !upper_ids_to_remove.remove(x))
                    .collect();
            }
            None => {
                snapshot.l0_sstables = snapshot
                    .l0_sstables
                    .iter()
                    .copied()
                    .filter(|x| !upper_ids_to_remove.remove(x))
                    .collect();
            }
        };

        assert!(upper_ids_to_remove.is_empty());

        // remove compacted lower level sst ids
        let mut lower_ids_to_remove = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        snapshot.levels[task.lower_level - 1].1 = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .copied()
            .filter(|x| !lower_ids_to_remove.remove(x))
            .collect();

        assert!(lower_ids_to_remove.is_empty());

        snapshot.levels[task.lower_level - 1].1.extend(output);

        if !in_recovery {
            // sort sstables in lower level after adding new sst
            snapshot.levels[task.lower_level - 1].1.sort_by(|x, y| {
                snapshot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(snapshot.sstables.get(y).unwrap().first_key())
            });
        }

        let mut files_to_remove =
            Vec::with_capacity(task.lower_level_sst_ids.len() + task.upper_level_sst_ids.len());

        files_to_remove.extend(&task.lower_level_sst_ids);
        files_to_remove.extend(&task.upper_level_sst_ids);

        (snapshot, files_to_remove)
    }
}
