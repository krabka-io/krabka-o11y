//! Complete cold log queries with broad and sparse fingerprint selections.

use std::{collections::BTreeMap, sync::Arc};

use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex, LogRow, TimeRange, labels,
    write_log_block_to_object_store, write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shards_to_object_store,
};
use krabka_logql::{StreamPlan, parse_query, plan_stream_query};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use serde_json::{Value, json};

use crate::Seeded;

/// Query shapes over the same persisted block.
pub const CASES: [&str; 9] = [
    "all",
    "all_exact",
    "all_line",
    "all_line_regex",
    "all_line_not_regex",
    "sparse",
    "sparse_window",
    "rare",
    "single",
];
/// Rows per stream; the maximum fixture has one million persisted rows.
pub const POINTS_PER_STREAM: usize = 10;
const TENANT: &str = "log-query-bench";

/// One deterministic block and its independent input ledger.
pub struct LogQueryFixture {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: ObjectPath,
    pub label_index: LabelIndex,
    block_index: LogBlockIndex,
    ledger: Vec<BTreeMap<String, String>>,
}

/// A production query plan and its complete expected Loki response.
pub struct LogQueryCase {
    pub plan: StreamPlan,
    pub expected: Value,
}

impl LogQueryFixture {
    /// Persist ten rows per stream with labels and nontrivial metadata.
    ///
    /// # Panics
    /// Panics if the count is zero, does not fit its types, or fixture writing fails.
    pub async fn new(streams: usize) -> Self {
        Self::new_in_store(streams, Arc::new(InMemory::new())).await
    }

    /// Persist the same fixture in a caller-supplied object store.
    ///
    /// # Panics
    /// Panics if the count is zero, does not fit its types, or fixture writing fails.
    pub async fn new_in_store(streams: usize, store: Arc<dyn ObjectStore>) -> Self {
        let streams = std::num::NonZeroUsize::new(streams)
            .expect("at least one stream")
            .get();
        let prefix = ObjectPath::from("logs");
        let mut label_index = LabelIndex::default();
        let mut ledger = Vec::with_capacity(streams);
        let mut rows = Vec::with_capacity(streams * POINTS_PER_STREAM);
        let mut random = Seeded::new(0x10C5_B10C);
        for stream in 0..streams {
            let source = labels([
                ("app", if stream % 4 == 0 { "api" } else { "worker" }),
                ("series", &format!("{stream:08}")),
                (
                    "bucket",
                    if stream.is_multiple_of(64) {
                        "rare"
                    } else {
                        "common"
                    },
                ),
            ]);
            let fingerprint = label_index.insert_series(TENANT, source.clone());
            let metadata = BTreeMap::from([(
                "payload".to_string(),
                format!(
                    "{:016x}{:016x}{:016x}{:016x}",
                    random.next_u64(),
                    random.next_u64(),
                    random.next_u64(),
                    random.next_u64()
                ),
            )]);
            let mut output_labels = source;
            output_labels.extend(metadata.clone());
            ledger.push(output_labels);
            for point in 0..POINTS_PER_STREAM {
                rows.push(LogRow::new(
                    fingerprint,
                    i64::try_from(point).expect("a timestamp fits i64"),
                    line(stream, point),
                    metadata.clone(),
                ));
            }
        }
        let range = TimeRange::new(0, 9).expect("a forward time range");
        let block = write_log_block_to_object_store(
            store.as_ref(),
            &prefix,
            &BlockKey::new(TENANT, 0, 0, 9, range),
            rows,
        )
        .await
        .expect("the fixture block writes");
        let mut block_index = LogBlockIndex::default();
        block_index.insert(block);
        Self {
            store,
            prefix,
            label_index,
            block_index,
            ledger,
        }
    }

    /// Persist the tenant indexes that frontend requests load and cache.
    ///
    /// # Panics
    /// Panics if writing the deterministic fixture manifest fails.
    pub async fn persist_indexes(&self) {
        write_tenant_log_index_manifest_to_object_store(
            self.store.as_ref(),
            &self.prefix,
            TENANT,
            &self.label_index,
            &self.block_index,
        )
        .await
        .expect("the fixture indexes write");
    }

    /// Persist one tenant shard containing all fixture rows.
    ///
    /// # Panics
    /// Panics if writing the deterministic fixture shard fails.
    pub async fn persist_shard(&self) {
        write_tenant_log_index_shards_to_object_store(
            self.store.as_ref(),
            &self.prefix,
            TENANT,
            &[TimeRange::new(0, 9).expect("the fixture range is valid")],
            &self.label_index,
            &self.block_index,
        )
        .await
        .expect("the fixture shard writes");
    }

    /// Physical block size for configuring a fixed number of frontend shards.
    #[must_use]
    pub fn block_bytes(&self) -> u64 {
        use krabka_units::convert::ByteSizeExt as _;
        self.block_index
            .blocks()
            .iter()
            .map(|block| block.size.bytes_u64())
            .sum()
    }

    /// Plan one query and build its response directly from generated inputs.
    ///
    /// # Panics
    /// Panics for an unknown case, an invalid fixture query, or an out-of-range timestamp.
    #[must_use]
    pub fn case(&self, name: &str) -> LogQueryCase {
        let (query, first, last) = match name {
            "all" => (r#"{app=~".+"}"#, 0, 9),
            "all_exact" => (r#"{app!=""}"#, 0, 9),
            "all_line" => (r#"{app=~".+"} |= "accepted""#, 0, 9),
            "all_line_regex" => (r#"{app!=""} |~ "accepted$""#, 0, 9),
            "all_line_not_regex" => (r#"{app!=""} !~ "ignored$""#, 0, 9),
            "sparse" => (r#"{app="api"}"#, 0, 9),
            "sparse_window" => (r#"{app="api"} |= "accepted""#, 2, 7),
            "rare" => (r#"{bucket="rare"}"#, 0, 9),
            "single" => (r#"{series="00000000"}"#, 0, 9),
            _ => panic!("unknown log query case"),
        };
        let plan = plan_stream_query(
            TENANT,
            TimeRange::new(first, last).expect("a forward range"),
            parse_query(query).expect("a fixture query parses"),
            &self.label_index,
            &self.block_index,
        )
        .expect("a fixture query plans");
        let mut result = BTreeMap::new();
        for (stream, output_labels) in self.ledger.iter().enumerate() {
            if (name.starts_with("sparse") && stream % 4 != 0)
                || (name == "rare" && !stream.is_multiple_of(64))
                || (name == "single" && stream != 0)
            {
                continue;
            }
            let values = (first..=last)
                .filter(|point| {
                    !matches!(
                        name,
                        "all_line" | "all_line_regex" | "all_line_not_regex" | "sparse_window"
                    ) || point % 2 == 0
                })
                .map(|point| {
                    json!([
                        point.to_string(),
                        line(stream, usize::try_from(point).expect("a point fits usize"))
                    ])
                })
                .collect::<Vec<_>>();
            result.insert(output_labels, values);
        }
        let result = result
            .into_iter()
            .map(|(stream, values)| json!({"stream":stream,"values":values}))
            .collect::<Vec<_>>();
        LogQueryCase {
            plan,
            expected: json!({"status":"success","data":{"resultType":"streams","result":result}}),
        }
    }
}

fn line(stream: usize, point: usize) -> String {
    format!(
        "stream={stream} point={point} {}",
        if point.is_multiple_of(2) {
            "accepted"
        } else {
            "ignored"
        }
    )
}
