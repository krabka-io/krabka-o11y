//! Search-space sharding, plus the by-id candidate enumeration.
//!
//! Sharding turns the candidate block set and the hot/cold frontier into a list
//! of bounded jobs, at the grain time -> shard -> block -> row-group.
//!
//! The shard grain matches what the querier in `querier/http` honors. A search
//! job restricts to one block and a row-group range, through `block`,
//! `rowGroupStart` and `rowGroupEnd`, which is the querier's
//! [`krabka_traceql::ScanJob`]. The live hot tier is the unrestricted scan. A
//! block larger than `target_per_job` fans into several row-group-range jobs.

use std::collections::BTreeMap;

use async_trait::async_trait;
use krabka_blockstore::{BlockStore, Result as BlockStoreResult, TraceIndex};
use krabka_units::{ByteSize, convert::ByteSizeExt};

#[cfg(test)]
mod tests {
    use krabka_units::bytes;

    use super::*;

    fn block(id: &str, start: i64, end: i64, rgs: &[u64]) -> BlockMetaInfo {
        let row_groups = rgs
            .iter()
            .enumerate()
            .map(|(i, &b)| RowGroupInfo {
                index: u32::try_from(i).unwrap(),
                compressed: ByteSize::from_bytes(b),
            })
            .collect();
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ns: start,
            end_ns: end,
            size: ByteSize::from_bytes(rgs.iter().sum()),
            row_groups,
        }
    }

    #[test]
    fn small_block_is_one_job_plus_live() {
        // Query window ends at 300, frontier 200 => window reaches hot.
        let blocks = vec![block("b1", 0, 100, &[500])];
        let plan = plan_search_jobs(&blocks, 300, 200, bytes(10_000));
        assert2::assert!(
            plan == JobPlan {
                jobs: vec![
                    JobShard::Live,
                    JobShard::Block {
                        block_id: "b1".to_string(),
                        row_group_start: 0,
                        row_group_end: 1,
                    },
                ],
                total_blocks: 1,
            }
        );
    }

    #[test]
    fn large_block_splits_into_row_group_jobs() {
        // size 30k > budget 10k, 3 row-groups => 3 row-group-range jobs, no Live
        // (query window ends at -10, before the frontier 0).
        let blocks = vec![block("b2", -1000, -10, &[10_000, 10_000, 10_000])];
        let plan = plan_search_jobs(&blocks, -10, 0, bytes(10_000));
        // Each job is a single-row-group range; no Live job.
        assert2::assert!(
            plan == JobPlan {
                jobs: vec![
                    JobShard::Block {
                        block_id: "b2".to_string(),
                        row_group_start: 0,
                        row_group_end: 1,
                    },
                    JobShard::Block {
                        block_id: "b2".to_string(),
                        row_group_start: 1,
                        row_group_end: 2,
                    },
                    JobShard::Block {
                        block_id: "b2".to_string(),
                        row_group_start: 2,
                        row_group_end: 3,
                    },
                ],
                total_blocks: 1,
            }
        );
    }

    #[test]
    fn empty_blocks_with_hot_window_is_just_live() {
        let blocks: Vec<BlockMetaInfo> = vec![];
        let plan = plan_search_jobs(&blocks, i64::MAX, 0, bytes(10_000));
        assert2::assert!(
            plan == JobPlan {
                jobs: vec![JobShard::Live],
                total_blocks: 0,
            }
        );
    }

    #[test]
    fn target_bytes_zero_never_splits() {
        let blocks = vec![block("b", 0, 10, &[10_000, 10_000, 10_000])];
        let plan = plan_search_jobs(&blocks, i64::MAX, 0, bytes(0));
        let rg_jobs: Vec<_> = plan
            .jobs
            .iter()
            .filter(|j| matches!(j, JobShard::Block { .. }))
            .collect();
        assert2::assert!(rg_jobs.len() == 1);
        assert2::assert!(matches!(
            rg_jobs[0],
            JobShard::Block {
                row_group_start: 0,
                row_group_end: 3,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn mock_catalog_returns_overlapping_blocks() {
        let cat = MockCatalog::new(vec![
            block("b1", 0, 100, &[500]),
            block("b2", 500, 600, &[500]),
        ]);
        let got = cat.blocks("t1", 0, 200).await.unwrap();
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].block_id.as_str() == "b1");
    }
}

mod block_catalog;
mod block_meta_info;
mod blocks_for_tenant;
mod catalog_error;
mod job_plan;
mod job_shard;
mod mock_catalog;
mod plan_block_jobs;
mod plan_search_jobs;
mod row_group_info;
mod trace_index_catalog;

pub use block_catalog::BlockCatalog;
pub use block_meta_info::BlockMetaInfo;
pub use blocks_for_tenant::blocks_for_tenant;
pub use catalog_error::CatalogError;
pub use job_plan::JobPlan;
pub use job_shard::JobShard;
pub use mock_catalog::MockCatalog;
use plan_block_jobs::plan_block_jobs;
pub use plan_search_jobs::plan_search_jobs;
pub use row_group_info::RowGroupInfo;
pub use trace_index_catalog::TraceIndexCatalog;
