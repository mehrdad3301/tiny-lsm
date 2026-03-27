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

        // check if levels is not empty 

        // trigger by space amplification ratio
        let all_levels_size = snapshot
            .levels 
            .iter()
            .map(|x| x.1.len())
            .sum::<usize>() as f64 ; 

        let last_level_size = snapshot.levels.last().unwrap().1.len() as f64 ;
        let all_levels_except_last_size = all_levels_size - last_level_size ; 

        if all_levels_except_last_size / last_level_size * 100.0 
                >= self.options.max_size_amplification_percent as f64 {

            return Some(TieredCompactionTask { 
                tiers: snapshot.levels.clone(), 
                bottom_tier_included: true, 
            })
        }

        // trigger by size ratio 

        // reduce sorted runs 

        None 
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {

        // assert that output is not empty 

        let mut snapshot = snapshot.clone() ;

        let mut files_to_remove: Vec<usize> = Vec::new() ; 

        let mut deleted_tier_ids = task 
            .tiers
            .iter()
            .map(|x| &x.0) 
            .copied()
            .collect::<HashSet<_>>() ; 

        files_to_remove.extend(deleted_tier_ids.iter()) ;

        let mut remaining_tiers = Vec::with_capacity(
            snapshot.levels.len() - files_to_remove.len()
        ) ; 

        for tier in snapshot.levels { 
            if !deleted_tier_ids.remove(&tier.0) { 
                remaining_tiers.push(tier.clone()) ;
            }
        } 

        snapshot.levels = remaining_tiers ; 

        snapshot.levels.push((output[0], output.to_vec())) ;

        (snapshot, files_to_remove)
    }
}
