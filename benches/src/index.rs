//! Populated series and trace indexes, for the pruning path.

use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::{
    BlockLevel, BlockMeta, Index, Labels, ShardedTraceBloom, TraceBlockStats, TraceIndex,
};

use crate::Seeded;

/// The tenant every fixture writes under.
pub const TENANT: &str = "bench";

/// How many distinct values each label of a generated series can take.
///
/// `pod` is deliberately unbounded -- it takes the series index itself -- so
/// that the cardinality of a fixture is the number of series asked for, and
/// the other labels stay a realistic, low-cardinality grouping over them.
/// That is the shape a real metrics workload has, and it is the shape that
/// makes an index either prune or not: a matcher on `job` selects a large
/// fraction of the series and a matcher on `pod` selects one.
const JOBS: usize = 16;
const INSTANCES: usize = 64;
const STATUSES: [&str; 4] = ["200", "404", "500", "503"];

/// The label set of series `which`.
///
/// Deterministic in `which` alone, so the same index is built by every caller
/// and by every run, and so `blocks::series_batches` and this module agree on
/// which fingerprints exist.
///
/// # Panics
/// Panics when a series index does not fit a `u64`.
#[must_use]
pub fn series_labels(which: usize) -> Labels {
    let mut pick = Seeded::new(u64::try_from(which).expect("a series index fits a u64"));
    let mut labels = Labels::new();
    labels.insert("__name__", "http_requests_total");
    labels.insert("job", format!("job-{}", pick.next_below(JOBS)));
    labels.insert(
        "instance",
        format!("instance-{}", pick.next_below(INSTANCES)),
    );
    labels.insert("status", STATUSES[pick.next_below(STATUSES.len())]);
    // The high-cardinality label, and the one a single-series matcher uses.
    labels.insert("pod", format!("pod-{which}"));
    labels
}

/// An [`Index`] holding `series` series spread over `blocks` blocks.
///
/// Each block covers a contiguous slice of the time range and holds every
/// series, which is what a block store fed by a scrape loop produces: blocks
/// are cut on time, not on series. A query for one series over part of the
/// range therefore has to prune on both axes, which is the work being measured.
///
/// # Panics
/// Panics when a block index does not fit an `i64`.
#[must_use]
pub fn populated_index(series: usize, blocks: usize) -> Index {
    let mut index = Index::new();
    let fingerprints: Vec<u64> = (0..series)
        .map(|which| {
            let labels = series_labels(which);
            let fingerprint = labels.fingerprint();
            index.add_series(TENANT, fingerprint, &labels);
            fingerprint
        })
        .collect();

    let span = BLOCK_SPAN_MS;
    for block in 0..blocks {
        let start = i64::try_from(block).expect("a block index fits an i64") * span;
        index.add_block(&BlockMeta {
            tenant: TENANT.to_string(),
            object_key: format!("blocks/{TENANT}/{block:06}.parquet"),
            min_ts: start,
            max_ts: start + span - 1,
            row_count: series,
            fingerprints: fingerprints.clone(),
            level: BlockLevel::INGESTED,
        });
    }
    index
}

/// The time a single block covers, in milliseconds. Two hours, as Prometheus
/// cuts them.
pub const BLOCK_SPAN_MS: i64 = 2 * 60 * 60 * 1_000;

/// A [`TraceIndex`] holding `blocks` blocks, each with `traces` trace ids.
///
/// Every block carries the same tag names and a slice of the tag values, so
/// `prune_blocks_by_tag` has something to reject: a value-scoped prune must
/// return the blocks that hold that value and no others, and a fixture where
/// every block holds every value would make any implementation look correct
/// and equally fast.
///
/// # Panics
/// Panics when a block index does not fit an `i64`.
#[must_use]
pub fn populated_trace_index(blocks: usize, traces: usize) -> TraceIndex {
    let mut index = TraceIndex::new();
    for block in 0..blocks {
        let start = i64::try_from(block).expect("a block index fits an i64") * BLOCK_SPAN_MS;
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(traces);
        for trace in 0..traces {
            bloom.insert(&trace_id(block, trace));
        }

        let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        tag_values.insert(
            "service.name".to_string(),
            (0..SERVICES_PER_BLOCK)
                .map(|slot| format!("svc-{}", (block + slot) % SERVICES))
                .collect(),
        );
        tag_values.insert(
            "http.status_code".to_string(),
            (0..4).map(|slot| format!("{}", 200 + slot * 100)).collect(),
        );

        index.add_trace_block(
            TENANT,
            TraceBlockStats {
                object_key: format!("traces/{TENANT}/{block:06}.parquet"),
                min_ts: start,
                max_ts: start + BLOCK_SPAN_MS - 1,
                bloom,
                tag_names: tag_values.keys().cloned().collect(),
                tag_values,
                row_count: 0,
                level: BlockLevel::INGESTED,
            },
        );
    }
    index
}

/// Distinct service names across the whole fixture, and how many of them any
/// one block holds. A prune on a service therefore selects a minority of the
/// blocks rather than all or one of them.
const SERVICES: usize = 32;
const SERVICES_PER_BLOCK: usize = 4;

/// The id of trace `trace` in block `block`, as the 16 bytes an index keys on.
///
/// # Panics
/// Panics when a block or trace index does not fit a `u64`.
#[must_use]
pub fn trace_id(block: usize, trace: usize) -> [u8; 16] {
    let mut id = [0_u8; 16];
    let high = u64::try_from(block).expect("a block index fits a u64");
    let low = u64::try_from(trace).expect("a trace index fits a u64");
    id[..8].copy_from_slice(&high.to_be_bytes());
    id[8..].copy_from_slice(&low.to_be_bytes());
    id
}
