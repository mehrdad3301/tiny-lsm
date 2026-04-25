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

use std::{cmp::max, collections::HashMap};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // trigger by space amplification ratio
        let all_levels_size = snapshot.levels.iter().map(|x| x.1.len()).sum::<usize>() as f64;

        let last_level_size = snapshot.levels.last().unwrap().1.len() as f64;
        let all_levels_except_last_size = all_levels_size - last_level_size;

        if all_levels_except_last_size / last_level_size * 100.0
            >= self.options.max_size_amplification_percent as f64
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // trigger by size ratio
        let size_ratio = (100 + self.options.size_ratio) as f64 / 100.0;
        let mut some_of_previous_tiers = snapshot.levels.first().unwrap().1.len();
        for (idx, (_, tier)) in snapshot.levels.iter().enumerate() {
            if idx == 0 {
                continue;
            }
            if tier.len() as f64 / some_of_previous_tiers as f64 > size_ratio
                && idx >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..idx].to_vec(),
                    bottom_tier_included: false,
                });
            }
            some_of_previous_tiers += tier.len();
        }

        // triggered by reducing sorted runs
        let idx = self
            .options
            .max_merge_width
            .unwrap_or(std::usize::MAX)
            .min(snapshot.levels.len());

        Some(TieredCompactionTask {
            tiers: snapshot.levels[..idx].to_vec(),
            bottom_tier_included: idx == snapshot.levels.len() - 1,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut files_to_remove: Vec<usize> = Vec::new();

        let mut deleted_tiers = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();

        let mut remaining_tiers = Vec::new();

        let mut new_tier_added = false;
        for tier in snapshot.levels {
            if let Some(file) = deleted_tiers.remove(&tier.0) {
                files_to_remove.extend(file.iter().copied());
            } else {
                remaining_tiers.push(tier.clone());
            }

            if !new_tier_added && deleted_tiers.is_empty() {
                new_tier_added = true;
                remaining_tiers.push((output[0], output.to_vec()));
            }
        }

        if !deleted_tiers.is_empty() {
            unreachable!("some tiers not found??");
        }

        snapshot.levels = remaining_tiers;

        (snapshot, files_to_remove)
    }
}
