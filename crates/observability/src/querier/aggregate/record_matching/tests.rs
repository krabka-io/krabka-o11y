use std::{num::NonZeroUsize, sync::Arc};

use assert2::assert;
use krabka_blockstore::{
    BlockKey, LabelPair, LogBlockIndex, LogRow, TimeRange, labels, write_log_block_to_object_store,
};
use krabka_logql::{parse_metric_query, plan_stream_query};
use object_store::{memory::InMemory, path::Path};
use serde_json::Value;

use super::*;
use crate::{
    ColdBlockScan, QueryHotTail,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
    querier::scan::{MetricEvaluationRange, MetricRangeScanRequest},
};

#[test]
fn metric_windows_match_linear_membership_at_boundaries_and_saturation() {
    let mut query = parse_metric_query(r#"count_over_time({app="api"}[1m])"#).unwrap();
    let times = [
        i64::MIN,
        i64::MIN + 1,
        -11,
        -10,
        -1,
        0,
        1,
        10,
        11,
        i64::MAX - 1,
        i64::MAX,
    ];
    for offset in [i64::MIN, -10, 0, 10, i64::MAX] {
        query.offset_ns.0 = offset;
        for range_ns in [0, 1, 10, i64::MAX] {
            for eval_times in [&times[..], &times[3..4], &times[..0]] {
                for timestamp_ns in times {
                    let mut samples = MetricSamples::new();
                    record_matching_metric_sample(
                        &mut samples,
                        MetricWindow {
                            query: &query,
                            eval_times,
                            range_ns,
                            delete_filters: &[],
                        },
                        MatchedEntry {
                            timestamp_ns,
                            metric_labels: Labels::new(),
                            line: String::new(),
                            unwrap_sample: None,
                        },
                    );
                    let actual = samples[&Labels::new()].keys().copied().collect::<Vec<_>>();
                    let expected = eval_times
                        .iter()
                        .copied()
                        .filter(|time| {
                            let end = time.saturating_sub(offset);
                            timestamp_ns > end.saturating_sub(range_ns) && timestamp_ns <= end
                        })
                        .collect::<Vec<_>>();
                    assert!(
                        actual == expected,
                        "timestamp={timestamp_ns}, offset={offset}, range={range_ns}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn metric_windows_produce_exact_hot_cold_and_mixed_results() {
    let source = labels([LabelPair::new("app", "api")]);
    // Include both open and closed edges, overlap, gaps, repeated timestamps,
    // and rows just outside the complete scan interval. Reverse input order
    // also exercises first/last aggregations independently of ingestion order.
    let records = [
        -1, 0, 1, 14, 15, 15, 16, 59, 60, 61, 74, 75, 76, 119, 120, 121, 180,
    ]
    .into_iter()
    .rev()
    .enumerate()
    .map(|(index, seconds)| WalLogRecord {
        tenant: "tenant".into(),
        labels: source.clone(),
        timestamp_ns: seconds * 1_000_000_000,
        line: format!("value={}", index + 1),
        structured_metadata: Labels::new(),
        position: None,
    })
    .collect::<Vec<_>>();
    for expression in [
        r#"count_over_time({app="api"}[1m])"#,
        r#"count_over_time({app="api"}[1m] offset 15s)"#,
        r#"bytes_over_time({app="api"}[1m] offset 15s)"#,
        r#"sum_over_time({app="api"} | logfmt | unwrap value [1m] offset 15s)"#,
        r#"first_over_time({app="api"} | logfmt | unwrap value [1m])"#,
        r#"last_over_time({app="api"} | logfmt | unwrap value [1m])"#,
        r#"quantile_over_time(0.5, {app="api"} | logfmt | unwrap value [1m])"#,
    ] {
        let query = parse_metric_query(expression).unwrap();
        let mut responses = Vec::new();
        for cold_count in [0, records.len() / 2, records.len()] {
            responses.push(tier_response(&query, &records, cold_count).await);
        }
        assert!(
            responses[0] == responses[1],
            "hot versus mixed: {expression}"
        );
        assert!(
            responses[0] == responses[2],
            "hot versus cold: {expression}"
        );
        assert!(
            !responses[0]["data"]["result"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

async fn tier_response(query: &MetricQuery, records: &[WalLogRecord], cold_count: usize) -> Value {
    let mut label_index = LabelIndex::default();
    let fingerprint = label_index.insert_series("tenant", records[0].labels.clone());
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("logs");
    let mut blocks = LogBlockIndex::default();
    if cold_count > 0 {
        blocks.insert(
            write_log_block_to_object_store(
                store.as_ref(),
                &prefix,
                &BlockKey::new(
                    "tenant",
                    0,
                    0,
                    0,
                    TimeRange::new(-1_000_000_000, 180_000_000_000).unwrap(),
                ),
                records[..cold_count]
                    .iter()
                    .map(|record| {
                        LogRow::new(
                            fingerprint,
                            record.timestamp_ns,
                            record.line.clone(),
                            Labels::new(),
                        )
                    })
                    .collect(),
            )
            .await
            .unwrap(),
        );
    }
    let plan = plan_stream_query(
        "tenant",
        TimeRange::new(-60_000_000_000, 180_000_000_000).unwrap(),
        query.stream.clone(),
        &label_index,
        &blocks,
    )
    .unwrap();
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        ColdBlockScan {
            store,
            prefix: &prefix,
            block_fetch_concurrency: NonZeroUsize::new(1).unwrap(),
        },
        MetricRangeScanRequest::new(
            &plan,
            query,
            &label_index,
            MetricEvaluationRange::new(
                TimeRange::new(60_000_000_000, 180_000_000_000).unwrap(),
                15_000_000_000,
            ),
            QueryHotTail {
                records: &records[cold_count..],
                frontier: &CompactionFrontier::new(i64::MIN),
                delete_filters: &[],
            },
        ),
    )
    .await
    .unwrap()
}
