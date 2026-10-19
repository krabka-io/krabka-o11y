//! One-minute count windows over deterministic rows on a fifteen-second grid.

use std::{collections::BTreeMap, sync::Arc};

use krabka_blockstore::{
    BlockKey, LabelIndex, LabelPair, LogBlockIndex, LogRow, TimeRange, labels,
    write_log_block_to_object_store,
};
use krabka_logql::{MetricQuery, StreamPlan, parse_metric_query, plan_stream_query};
use krabka_observability::execute_metric_query_range_from_object_store;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use serde_json::{Value, json};

/// The evaluation interval, in nanoseconds.
const STEP_NS: i64 = 15_000_000_000;
const RANGE_NS: i64 = 60_000_000_000;

/// A persisted block, production plan and independent complete-response oracle.
pub struct MetricWindowFixture {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    plan: StreamPlan,
    query: MetricQuery,
    labels: LabelIndex,
    evaluation: TimeRange,
    /// Complete response counted directly from the generated row timestamps.
    pub expected: Value,
}

impl MetricWindowFixture {
    /// Build outside the measurement, spreading rows over the evaluation span.
    ///
    /// # Panics
    /// Panics for invalid sizes or when fixture persistence or planning fails.
    pub async fn new(rows: usize, steps: usize) -> Self {
        assert!(rows > 1 && steps > 0, "at least two rows and one step");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("metric-windows");
        let mut labels_index = LabelIndex::default();
        let source = labels([LabelPair::new("app", "api")]);
        let fingerprint = labels_index.insert_series("bench", source.clone());
        let end = RANGE_NS + i64::try_from(steps - 1).unwrap() * STEP_NS;
        let timestamps = (0..rows)
            .map(|row| {
                i64::try_from(
                    i128::try_from(row).unwrap() * i128::from(end)
                        / i128::try_from(rows - 1).unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let block = write_log_block_to_object_store(
            store.as_ref(),
            &prefix,
            &BlockKey::new("bench", 0, 0, 0, TimeRange::new(0, end).unwrap()),
            timestamps
                .iter()
                .map(|time| LogRow::new(fingerprint, *time, "line", BTreeMap::new()))
                .collect(),
        )
        .await
        .unwrap();
        let mut blocks = LogBlockIndex::default();
        blocks.insert(block);
        let query = parse_metric_query(r#"count_over_time({app="api"}[1m])"#).unwrap();
        let plan = plan_stream_query(
            "bench",
            TimeRange::new(0, end).unwrap(),
            query.stream.clone(),
            &labels_index,
            &blocks,
        )
        .unwrap();
        // Count rows per window directly from the input ledger. This oracle
        // never selects evaluation windows for a row.
        let values = (0..steps)
            .map(|step| {
                let time = RANGE_NS + i64::try_from(step).unwrap() * STEP_NS;
                let count = timestamps.partition_point(|row| *row <= time)
                    - timestamps.partition_point(|row| *row <= time - RANGE_NS);
                json!([time / 1_000_000_000, count.to_string()])
            })
            .collect::<Vec<_>>();
        Self {
            store,
            prefix,
            plan,
            query,
            labels: labels_index,
            evaluation: TimeRange::new(RANGE_NS, end).unwrap(),
            expected: json!({"status":"success","data":{"resultType":"matrix","result":[{"metric":source,"values":values}]}}),
        }
    }

    /// Execute the complete production cold query without a frontend cache.
    ///
    /// # Panics
    /// Panics when query execution fails.
    pub async fn execute(&self) -> Value {
        execute_metric_query_range_from_object_store(
            Arc::clone(&self.store),
            &self.prefix,
            &self.plan,
            &self.query,
            &self.labels,
            self.evaluation,
            STEP_NS,
        )
        .await
        .unwrap()
    }
}
